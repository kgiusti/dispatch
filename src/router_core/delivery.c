/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


#include <qpid/dispatch/delivery.h>
#include "delivery_core.h"
#include "delivery_io.h"

#include <inttypes.h>

typedef struct qdr_delivery_in_t qdr_delivery_in_t;
typedef struct qdr_delivery_out_t qdr_delivery_out_t;

DEQ_DECLARE(qdr_delivery_out_t, qdr_delivery_out_list_t);

/**
 * An Inbound delivery (received by router)
 *
 * Notes:
 * - transfer lock must be held when out_dlvs are referenced as they may be
 *   owned by other I/O threads
 * - an inbound delivery must exist until all outbound deliveries have been
 *   cleaned up (since the inbound has the lock).  So the inbound delivery is
 *   refcounted by all outbound deliveries
 */
struct qdr_delivery_in_t {
    qdr_delivery_t           base;

    bool                     send_to_core; // keep sending rx data to core until false
    qdr_delivery_out_list_t  out_dlvs;     // corresponding outbound deliveries
};
ALLOC_DECLARE(qdr_delivery_in_t);
ALLOC_DEFINE(qdr_delivery_in_t);


/**
 * An Outbound delivery (sent from router)
 *
 * Notes:
 * - in_dlv will be null if this outbound delivery is for a core link endpoint
 *   outgoing message
 * - the inbound delivery in_dlv is refcounted so it remains present until the
 *   out_dlv releases it
 *
 */
struct qdr_delivery_out_t {
    qdr_delivery_t          base;

    qdr_delivery_in_t      *in_dlv;       // corresponding inbound delivery
    DEQ_LINKS(qdr_delivery_out_t);        // peers of same in_dlv
};
ALLOC_DECLARE(qdr_delivery_out_t);
ALLOC_DEFINE(qdr_delivery_out_t);


static void qdr_delivery_update_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_delete_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_deliver_continue_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static pn_data_t *qdr_delivery_extension_state(qdr_delivery_t *delivery);
static void qdr_delivery_free_extension_state(qdr_delivery_t *delivery);
static void qdr_delete_delivery_internal_CT(qdr_core_t *core, qdr_delivery_t *delivery);
static void qdr_delivery_continue_transfer(qdr_core_t *core, qdr_delivery_t *in_delivery);
static void qdr_delivery_lock_transfer(qdr_delivery_t *delivery);
static void qdr_delivery_unlock_transfer(qdr_delivery_t *delivery);


/**
 * Constructor
 */
qdr_delivery_t *qdr_delivery(qdr_link_t *link)
{
    assert(link);
    if (link->link_direction == QD_INCOMING) {
        qdr_delivery_in_t *in_dlv = new_qdr_delivery_in_t();
        ZERO(in_dlv);
        set_safe_ptr_qdr_link_t(link, &in_dlv->base.link_sp);
        in_dlv->base.incoming = true;
        in_dlv->send_to_core = true;
        return &in_dlv->base;
    } else {
        qdr_delivery_out_t *out_dlv = new_qdr_delivery_out_t();
        ZERO(out_dlv);
        set_safe_ptr_qdr_link_t(link, &out_dlv->base.link_sp);
        return &out_dlv->base;
    }
}


void qdr_delivery_set_context(qdr_delivery_t *delivery, void *context)
{
    delivery->context = context;
}


void *qdr_delivery_get_context(const qdr_delivery_t *delivery)
{
    return delivery->context;
}

qdr_link_t *qdr_delivery_link(const qdr_delivery_t *delivery)
{
    return delivery ? safe_deref_qdr_link_t(delivery->link_sp) : 0;
}


bool qdr_delivery_send_complete(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_send_complete(delivery->msg);
}


bool qdr_delivery_tag_sent(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_tag_sent(delivery->msg);
}


void qdr_delivery_set_tag_sent(const qdr_delivery_t *delivery, bool tag_sent)
{
    if (!delivery)
        return;

    qd_message_set_tag_sent(delivery->msg, tag_sent);
}


bool qdr_delivery_receive_complete(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_receive_complete(delivery->msg);
}


void qdr_delivery_set_disposition(qdr_delivery_t *delivery, uint64_t disposition)
{
    if (delivery)
        delivery->disposition = disposition;
}


uint64_t qdr_delivery_disposition(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return 0;
    return delivery->disposition;
}


bool qdr_delivery_is_settled(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return delivery->settled;
}


void qdr_delivery_incref(qdr_delivery_t *delivery, const char *label)
{
    uint32_t rc = sys_atomic_inc(&delivery->ref_count);
    assert(rc > 0 || !delivery->ref_counted);
    delivery->ref_counted = true;
    qdr_link_t *link = qdr_delivery_link(delivery);
    if (link)
        qd_log(link->core->log, QD_LOG_DEBUG,
               "Delivery incref:    dlv:%lx rc:%"PRIu32" %s", (long) delivery, rc + 1, label);
}


void qdr_delivery_set_aborted(qdr_delivery_t *delivery, bool aborted)
{
    assert(delivery);
    qd_message_set_aborted(delivery->msg, aborted);
}

bool qdr_delivery_is_aborted(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_aborted(delivery->msg);
}


void qdr_delivery_decref(qdr_core_t *core, qdr_delivery_t *delivery, const char *label)
{
    uint32_t ref_count = sys_atomic_dec(&delivery->ref_count);
    assert(ref_count > 0);
    qd_log(core->log, QD_LOG_DEBUG, "Delivery decref:    dlv:%lx rc:%"PRIu32" %s", (long) delivery, ref_count - 1, label);

    if (ref_count == 1) {
        //
        // The delivery deletion must occur inside the core thread.
        // Queue up an action to do the work.
        //
        qdr_action_t *action = qdr_action(qdr_delete_delivery_CT, "delete_delivery");
        action->args.delivery.delivery = delivery;
        action->label = label;
        qdr_action_enqueue(core, action);
    }
}


void qdr_delivery_tag(const qdr_delivery_t *delivery, const char **tag, int *length)
{
    *tag    = (const char*) delivery->tag;
    *length = delivery->tag_length;
}


qd_message_t *qdr_delivery_message(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return 0;
    return delivery->msg;
}


qdr_error_t *qdr_delivery_error(const qdr_delivery_t *delivery)
{
    return delivery->error;
}


bool qdr_delivery_is_presettled(const qdr_delivery_t *delivery)
{
    return delivery->presettled;
}


// Called from I/O thread. Copy the state from dlv to its peer and schedule the
// peer's I/O thread if necessary.
// Assumes qdr_delivery_lock_transfer is held
//
static bool qdr_delivery_update_peer_LH(qdr_delivery_t *peer, uint64_t disposition, bool settled,
                                        pn_data_t *disposition_data, qdr_error_t *error)
{
    bool update = false;
    bool error_assigned = false;

    if (!peer)
        return false;

    if (disposition && disposition != peer->disposition) {
        peer->disposition = disposition;
        // TODO(kgiusti) fix needed for unsettled multicast:
        // do all peers need a copy?
        if (error) {
            qdr_error_free(peer->error);
            peer->error = error;
            error_assigned = true;
        }
        qdr_delivery_read_extension_state(peer, disposition, disposition_data);
        update = true;
    }

    if (settled && !peer->settled) {
        peer->settled = true;
        update = true;
    }

    // note: the current I/O thread is attempting to modify the qdr_link_t and
    // qdr_connection_t on another thread (the peer's).  We hold the transfer
    // lock during this process to prevent them from being cleaned up while
    // we're modifying them.

    if (update) {
        qdr_link_t *link = qdr_delivery_link(peer);
        if (link) {
            qdr_connection_t *conn = link->conn;
            if (conn) {
                qdr_delivery_incref(peer, "qdr_delivery_update_peer_LH - add to updated list");
                sys_mutex_lock(conn->work_lock);
                qdr_add_delivery_ref(&link->updated_deliveries, peer);
                // Adding this work at priority 0.
                qdr_add_link_ref(conn->links_with_work, link, QDR_LINK_LIST_CLASS_WORK);
                sys_mutex_unlock(conn->work_lock);

                qd_server_activate(qdr_connection_get_context(conn));
            }
        }
    }

    return error_assigned;
}


/**
 * Proton notified us that the remote has either settled or updated its
 * disposition or both.  Update the qdr_delivery_t state and propagate any
 * changes "downstream".  If the delivery has been settled notify Core.
 * Runs on dlv's I/O thread.
 *
 * @return: true if the proton delivery can be locally settled
 */
bool qdr_delivery_update_disposition(qdr_core_t *core, qdr_delivery_t *delivery, uint64_t disposition,
                                     bool settled, qdr_error_t *error, pn_data_t *ext_state)
{
    bool error_unassigned = true;
    bool update_peer = false;
    bool settle_pn_dlv = false;
    bool decref = false;

    qdr_delivery_lock_transfer(delivery);

    if (disposition && delivery->disposition != disposition) {
        update_peer = true;
        delivery->disposition = disposition;
        // update delivery-state extensions e.g. declared, transactional-state
        qdr_delivery_read_extension_state(delivery, disposition, ext_state);
    }

    if (settled && !delivery->settled) {
        update_peer = true;
        delivery->settled = true;
    }

    if (delivery->incoming) {

        // for inbound delay local settlement until message rx is done
        settle_pn_dlv = settled && qdr_delivery_receive_complete(delivery);

        if (update_peer) {

            // copy state downstream
            qdr_delivery_in_t *in_dlv = (qdr_delivery_in_t *)delivery;
            qdr_delivery_out_t *peer = DEQ_HEAD(in_dlv->out_dlvs);
            while (peer) {
                if (qdr_delivery_update_peer_LH(&peer->base, delivery->disposition, delivery->settled,
                                                qdr_delivery_extension_state(delivery), error)) {
                    // TODO(kgiusti) fix needed for unsettled multicast:
                    // do all peers need a copy?
                    error = 0;
                    error_unassigned = false;
                }
                peer = DEQ_NEXT(peer);
            }

            qdr_delivery_unlock_transfer(delivery);
        }

    } else {  // delivery is outbound

        // if the remote has settled it won't accept any more frames so we do
        // not care if the message send is complete or not:
        settle_pn_dlv = settled;

        if (update_peer) {

            qdr_delivery_lock_transfer(delivery);   // may be unlinking delivery

            qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)delivery;
            qdr_delivery_in_t *peer = out_dlv->in_dlv;
            if (peer) {

                // only update/settle if last outbound
                if (DEQ_SIZE(peer->out_dlvs) == 1) {
                    if (qdr_delivery_update_peer_LH(&peer->base, delivery->disposition, delivery->settled,
                                                    qdr_delivery_extension_state(delivery), error)) {
                        error = 0;
                        error_unassigned = false;
                    }
                }

                if (settled) {
                    // the pn_delivery_t has been settled, unlink delivery from peer.
                    DEQ_REMOVE(peer->out_dlvs, out_dlv);
                    out_dlv->in_dlv = 0;
                    decref = true;
                }
            }

            qdr_delivery_unlock_transfer(delivery);

            if (decref) {
                qdr_delivery_decref(core, &peer->base, "qdr_delivery_update_disposition - drop ref to peer in dlv");
            }
        }
    }

    if (settle_pn_dlv) {
        // notify Core of settlement
        qdr_action_t *action = qdr_action(qdr_delivery_update_CT, "update_delivery");
        action->args.delivery.delivery = delivery;
        qdr_delivery_incref(delivery, "qdr_delivery_update_disposition - add to action list");

        qdr_action_enqueue(core, action);
    }

    if (decref)
        qdr_delivery_decref(core, delivery, "qdr_delivery_update_disposition - out_dlv remote settled");

    if (error_unassigned)
        qdr_error_free(error);

    return settle_pn_dlv;
}


// More data has arrived for an incoming message. Schedule all peer outbound
// deliveries for I/O and possibly forward to core for in-process subscribers.
// Runs on I/O thread.
//
// @return: true if delivery can be settled
//
void qdr_deliver_continue(qdr_core_t *core, qdr_delivery_t *in_dlv)
{
    bool rx_done = qd_message_receive_complete(in_dlv->msg);

    // schedule all out_dlvs
    qdr_delivery_continue_transfer(core, in_dlv);

    // forward to core for in-process subscribers
    if (((qdr_delivery_in_t *)in_dlv)->send_to_core) {
        qdr_action_t *action = qdr_action(qdr_deliver_continue_CT, "deliver_continue");

        action->args.connection.delivery = in_dlv;
        action->args.connection.more = !rx_done;

        // This incref is for the action reference
        qdr_delivery_incref(in_dlv, "qdr_deliver_continue - add to action list");
        qdr_action_enqueue(core, action);
    }

    // if we've been waiting for rx to complete so we can settle, now's the
    // time:
    if (rx_done) {

        // note: qdr_delivery_update_peer_LH will set both settled and
        // disposition under lock.  We need to be sure that we're consistent
        // otherwise we may settle before the disposition change becomes
        // visible
        qdr_delivery_lock_transfer(in_dlv);

        bool settled = in_dlv->settled;
        uint64_t disp = in_dlv->disposition;

        qdr_delivery_unlock_transfer(in_dlv);

        if (settled) {
            settled = core->delivery_update_handler(core->user_context, in_dlv, disp, settled, false);
            if (settled) {
                // notify core
                qdr_action_t *action = qdr_action(qdr_delivery_update_CT, "update_delivery");
                action->args.delivery.delivery = in_dlv;
                qdr_action_enqueue(core, action);
            }
        }
    }
}


void qdr_delivery_release_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    bool push = false;
    bool moved = false;

    if (dlv->presettled) {
        //
        // The delivery is presettled. We simply want to call CORE_delivery_update which in turn will
        // restart stalled links if the q2_holdoff has been hit.
        // For single frame presettled deliveries, calling CORE_delivery_update does not do anything.
        //
        push = true;
    }
    else {
        push = dlv->disposition != PN_RELEASED;
        dlv->disposition = PN_RELEASED;
        dlv->settled = true;
        moved = qdr_delivery_settled_CT(core, dlv);

    }

    if (push || moved)
        qdr_delivery_push_CT(core, dlv);

    //
    // Remove the unsettled reference
    //
    if (moved)
        qdr_delivery_decref_CT(core, dlv, "qdr_delivery_release_CT - remove from unsettled list");
}


void qdr_delivery_failed_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    bool push = dlv->disposition != PN_MODIFIED;

    dlv->disposition = PN_MODIFIED;
    dlv->settled = true;
    bool moved = qdr_delivery_settled_CT(core, dlv);

    if (push || moved)
        qdr_delivery_push_CT(core, dlv);

    //
    // Remove the unsettled reference
    //
    if (moved)
        qdr_delivery_decref_CT(core, dlv, "qdr_delivery_failed_CT - remove from unsettled list");
}


bool qdr_delivery_settled_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    //
    // Remove a delivery from its unsettled list.  Side effects include issuing
    // replacement credit and visiting the link-quiescence algorithm
    //
    qdr_link_t       *link  = qdr_delivery_link(dlv);
    qdr_connection_t *conn  = link ? link->conn : 0;
    bool              moved = false;

    if (!link || !conn)
        return false;

    //
    // The lock needs to be acquired only for outgoing links
    //
    if (link->link_direction == QD_OUTGOING)
        sys_mutex_lock(conn->work_lock);

    if (dlv->where == QDR_DELIVERY_IN_UNSETTLED) {
        DEQ_REMOVE(link->unsettled, dlv);
        dlv->where = QDR_DELIVERY_NOWHERE;
        moved = true;
    }

    if (link->link_direction == QD_OUTGOING)
        sys_mutex_unlock(conn->work_lock);

    if (dlv->tracking_addr) {
        dlv->tracking_addr->outstanding_deliveries[dlv->tracking_addr_bit]--;
        dlv->tracking_addr->tracked_deliveries--;

        if (dlv->tracking_addr->tracked_deliveries == 0)
            qdr_check_addr_CT(core, dlv->tracking_addr);

        dlv->tracking_addr = 0;
    }

    //
    // If this is an incoming link and it is not link-routed or inter-router, issue
    // one replacement credit on the link.  Note that credit on inter-router links is
    // issued immediately even for unsettled deliveries.
    //
    if (moved && link->link_direction == QD_INCOMING &&
        link->link_type != QD_LINK_ROUTER && !link->edge && !link->connected_link)
        qdr_link_issue_credit_CT(core, link, 1, false);

    return moved;
}


static void qdr_delivery_increment_counters_CT(qdr_core_t *core, qdr_delivery_t *delivery)
{
    qdr_link_t *link = qdr_delivery_link(delivery);
    if (link) {
        bool do_rate = false;

        if (delivery->presettled) {
            do_rate = delivery->disposition != PN_RELEASED;
            link->presettled_deliveries++;
            if (link->link_direction ==  QD_INCOMING && link->link_type == QD_LINK_ENDPOINT)
                core->presettled_deliveries++;
        }
        else if (delivery->disposition == PN_ACCEPTED) {
            do_rate = true;
            link->accepted_deliveries++;
            if (link->link_direction ==  QD_INCOMING)
                core->accepted_deliveries++;
        }
        else if (delivery->disposition == PN_REJECTED) {
            do_rate = true;
            link->rejected_deliveries++;
            if (link->link_direction ==  QD_INCOMING)
                core->rejected_deliveries++;
        }
        else if (delivery->disposition == PN_RELEASED && !delivery->presettled) {
            link->released_deliveries++;
            if (link->link_direction ==  QD_INCOMING)
                core->released_deliveries++;
        }
        else if (delivery->disposition == PN_MODIFIED) {
            link->modified_deliveries++;
            if (link->link_direction ==  QD_INCOMING)
                core->modified_deliveries++;
        }

        uint32_t delay = core->uptime_ticks - delivery->ingress_time;
        if (delay > 10) {
            link->deliveries_delayed_10sec++;
            if (link->link_direction ==  QD_INCOMING)
                core->deliveries_delayed_10sec++;
        } else if (delay > 1) {
            link->deliveries_delayed_1sec++;
            if (link->link_direction ==  QD_INCOMING)
                core->deliveries_delayed_1sec++;
        }

        if (qd_bitmask_valid_bit_value(delivery->ingress_index) && link->ingress_histogram)
            link->ingress_histogram[delivery->ingress_index]++;

        //
        // Compute the settlement rate
        //
        if (do_rate) {
            uint32_t delta_time = core->uptime_ticks - link->core_ticks;
            if (delta_time > 0) {
                if (delta_time > QDR_LINK_RATE_DEPTH)
                    delta_time = QDR_LINK_RATE_DEPTH;
                for (uint8_t delta_slots = 0; delta_slots < delta_time; delta_slots++) {
                    link->rate_cursor = (link->rate_cursor + 1) % QDR_LINK_RATE_DEPTH;
                    link->settled_deliveries[link->rate_cursor] = 0;
                }
                link->core_ticks = core->uptime_ticks;
            }
            link->settled_deliveries[link->rate_cursor]++;
        }
    }
}


static void qdr_delete_delivery_internal_CT(qdr_core_t *core, qdr_delivery_t *delivery)
{
    assert(sys_atomic_get(&delivery->ref_count) == 0);

    if (delivery->msg || delivery->to_addr) {
        qdr_delivery_cleanup_t *cleanup = new_qdr_delivery_cleanup_t();

        DEQ_ITEM_INIT(cleanup);
        cleanup->msg  = delivery->msg;
        cleanup->iter = delivery->to_addr;

        DEQ_INSERT_TAIL(core->delivery_cleanup_list, cleanup);
    }

    if (delivery->tracking_addr) {
        delivery->tracking_addr->outstanding_deliveries[delivery->tracking_addr_bit]--;
        delivery->tracking_addr->tracked_deliveries--;

        if (delivery->tracking_addr->tracked_deliveries == 0)
            qdr_check_addr_CT(core, delivery->tracking_addr);

        delivery->tracking_addr = 0;
    }

    qdr_delivery_increment_counters_CT(core, delivery);

    qd_bitmask_free(delivery->link_exclusion);
    qdr_error_free(delivery->error);

    if (delivery->incoming) {
        qdr_delivery_in_t *in_dlv = (qdr_delivery_in_t *)delivery;
        assert(DEQ_SIZE(in_dlv->out_dlvs) == 0);
        free_qdr_delivery_in_t(in_dlv);

    } else {
        qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)delivery;
        assert(!out_dlv->in_dlv);
        free_qdr_delivery_out_t(out_dlv);
    }
}


/**
 * Set the in delivery's corresponding outbound delivery.
 * Used in the non-multicast case only.
 */
bool qdr_delivery_set_outgoing_CT(qdr_core_t *core, qdr_delivery_t *in_delivery, qdr_delivery_t *out_delivery)
{
    // If there is no delivery or a peer, we cannot link each other.
    if (!in_delivery || !out_delivery)
        return false;

    assert(in_delivery->incoming && !in_delivery->multicast);
    assert(!out_delivery->incoming);

    qdr_delivery_in_t  *in_dlv  = (qdr_delivery_in_t *)in_delivery;
    qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)out_delivery;

    // Create peer linkage when:
    // 1) the outgoing delivery is unsettled. This peer linkage is necessary
    // for propagating dispositions from the consumer back to the sender.
    // 2) if the message is not yet been completely received. This linkage will
    // help us stream large pre-settled messages.
    //
    if (!out_dlv->base.settled || !qd_message_receive_complete(out_dlv->base.msg)) {

        qdr_delivery_lock_transfer(in_delivery);

        DEQ_INSERT_TAIL(in_dlv->out_dlvs, out_dlv);
        qdr_delivery_incref(&out_dlv->base, "qdr_delivery_set_outgoing_CT - linked to peer (out delivery)");
        assert(!out_dlv->in_dlv);
        out_dlv->in_dlv = in_dlv;
        qdr_delivery_incref(&in_dlv->base, "qdr_delivery_set_outgoing_CT - linked to peer (in delivery)");

        qdr_delivery_unlock_transfer(in_delivery);

        return true;
    }
    return false;
}


/**
 * atomically set all outgoing delivery peers of an incoming multicast delivery
 */
void qdr_delivery_set_mcasts_CT(qdr_core_t *core, qdr_delivery_t *in_delivery, const qdr_delivery_mcast_list_t *out_deliveries)
{
    if (!in_delivery) {
        // core generated outbound message, these are simply forwarded
        return;
    }

    assert(in_delivery->incoming && in_delivery->multicast);

    // Only link if this is a large streaming message
    // TODO(kgiusti): for unsettled mcast need to link if unsettled
    if (qd_message_receive_complete(in_delivery->msg))
        return;

    qdr_delivery_lock_transfer(in_delivery);

    qdr_delivery_in_t  *in_dlv  = (qdr_delivery_in_t *)in_delivery;

    qdr_delivery_mcast_node_t *node = DEQ_HEAD(*out_deliveries);
    while (node) {
        qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)node->out_dlv;
        assert(out_dlv && !out_dlv->base.incoming && !out_dlv->in_dlv);
        DEQ_INSERT_TAIL(in_dlv->out_dlvs, out_dlv);
        qdr_delivery_incref(&out_dlv->base, "qdr_delivery_set_mcasts_CT - added to in delivery's out_dlvs");
        out_dlv->in_dlv = in_dlv;
        qdr_delivery_incref(&in_dlv->base, "qdr_delivery_set_mcasts_CT - linked to out delivery");
        node = DEQ_NEXT(node);
    }

    qdr_delivery_unlock_transfer(in_delivery);

}


static void qdr_delivery_unlink_peers_CT(qdr_core_t *core, qdr_delivery_t *delivery, qdr_delivery_t *peer)
{
    if (!peer || !delivery)
        return;

    qdr_delivery_in_t  *in_dlv; 
    qdr_delivery_out_t *out_dlv;

    if (delivery->incoming) {
        in_dlv  = (qdr_delivery_in_t *) delivery;
        out_dlv = (qdr_delivery_out_t *) peer;
    } else {
        in_dlv  = (qdr_delivery_in_t *) peer;
        out_dlv = (qdr_delivery_out_t *) delivery;
    }
    assert(out_dlv->in_dlv == in_dlv);

    out_dlv->in_dlv = 0;
    DEQ_REMOVE(in_dlv->out_dlvs, out_dlv);

    qdr_delivery_decref_CT(core, &in_dlv->base, "qdr_delivery_unlink_peers_CT - unlink in_dlv");
    qdr_delivery_decref_CT(core, &out_dlv->base, "qdr_delivery_unlink_peers_CT - unlink out_dlv");
}


void qdr_delivery_decref_CT(qdr_core_t *core, qdr_delivery_t *dlv, const char *label)
{
    uint32_t ref_count = sys_atomic_dec(&dlv->ref_count);
    qd_log(core->log, QD_LOG_DEBUG, "Delivery decref_CT: dlv:%lx rc:%"PRIu32" %s", (long) dlv, ref_count - 1, label);
    assert(ref_count > 0);

    if (ref_count == 1)
        qdr_delete_delivery_internal_CT(core, dlv);
}


/**
 * Core handling of proton delivery settlement
 */
static void qdr_delivery_update_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_delivery_t *dlv = action->args.delivery.delivery;

    if (discard)
        return;

    if (qdr_delivery_settled_CT(core, dlv))
        qdr_delivery_decref_CT(core, dlv, "qdr_delivery_update_CT - dlv removed from unsettled");
    //
    // Release the action reference, possibly freeing the delivery
    //
    qdr_delivery_decref_CT(core, dlv, "qdr_update_delivery_CT - remove from action");
}


static void qdr_delete_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard)
        qdr_delete_delivery_internal_CT(core, action->args.delivery.delivery);
}


// schedule outbound deliveries for work - thread safe
//
static void qdr_delivery_continue_transfer(qdr_core_t *core, qdr_delivery_t *in_delivery)
{
    assert(in_delivery->incoming);

    qdr_delivery_lock_transfer(in_delivery);

    // the out_dlvs list is set atomically when the core has finished
    // forwarding.  So if the core hasn't forwarded it yet, nothing here gets
    // done.
    qdr_delivery_in_t  *in_dlv = (qdr_delivery_in_t *)in_delivery;
    qdr_delivery_out_t *out_dlv = DEQ_HEAD(in_dlv->out_dlvs);
    while (out_dlv) {
        qdr_link_work_t *work     = out_dlv->base.link_work;
        qdr_link_t      *out_link = qdr_delivery_link(&out_dlv->base);

        //
        // Determine if the out connection can be activated.
        // For a large message, the outgoing delivery's link_work MUST be at
        // the head of the outgoing link's work list. This link work is only
        // removed after the streaming message has been sent.
        //
        if (!!work && !!out_link) {
            sys_mutex_lock(out_link->conn->work_lock);
            if (work->processing || work == DEQ_HEAD(out_link->work_list)) {
                // Adding this work at priority 0.
                qdr_add_link_ref(out_link->conn->links_with_work, out_link, QDR_LINK_LIST_CLASS_WORK);
                sys_mutex_unlock(out_link->conn->work_lock);

                //
                // Activate the outgoing connection for later processing.
                //
                qdr_connection_activate(core, out_link->conn);
            }
            else
                sys_mutex_unlock(out_link->conn->work_lock);
        }

        out_dlv = DEQ_NEXT(out_dlv);
    }

    qdr_delivery_unlock_transfer(in_delivery);
}


// More message data has arrived on the inbound delivery that has in-process consumers.
//
static void qdr_deliver_continue_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (discard)
        return;

    qdr_delivery_t *in_dlv = action->args.connection.delivery;
    if (((qdr_delivery_in_t *)in_dlv)->send_to_core) {

        bool            more = action->args.connection.more;
        qdr_link_t     *link = qdr_delivery_link(in_dlv);
        qd_message_t   *msg = qdr_delivery_message(in_dlv);

        if (!!link && !qd_message_is_discard(msg)) {

            if (!more) {
                //
                // The entire message has now been received. Check to see if there are in process subscriptions that need to
                // receive this message. in process subscriptions, at this time, can deal only with full messages.
                //
                qdr_subscription_t *sub = DEQ_HEAD(in_dlv->subscriptions);
                while (sub) {
                    DEQ_REMOVE_HEAD(in_dlv->subscriptions);
                    qdr_forward_on_message_CT(core, sub, link, in_dlv->msg);
                    sub = DEQ_HEAD(in_dlv->subscriptions);
                }
            }

            if (link->core_endpoint)
                qdrc_endpoint_do_deliver_CT(core, link->core_endpoint, in_dlv);
        }
    }

    // This decref is for the action reference
    qdr_delivery_decref_CT(core, in_dlv, "qdr_deliver_continue_CT - remove from action list");
}


void qdr_delivery_push_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    qdr_link_t *link = qdr_delivery_link(dlv);
    if (!link)
        return;

    bool activate = false;

    sys_mutex_lock(link->conn->work_lock);
    if (dlv->where != QDR_DELIVERY_IN_UNDELIVERED) {
        qdr_delivery_incref(dlv, "qdr_delivery_push_CT - add to updated list");
        qdr_add_delivery_ref(&link->updated_deliveries, dlv);
        // Adding this work at priority 0.
        qdr_add_link_ref(link->conn->links_with_work, link, QDR_LINK_LIST_CLASS_WORK);
        activate = true;
    }
    sys_mutex_unlock(link->conn->work_lock);

    //
    // Activate the connection
    //
    if (activate)
        qdr_connection_activate(core, link->conn);
}

pn_data_t* qdr_delivery_extension_state(qdr_delivery_t *delivery)
{
    if (!delivery->extension_state) {
        delivery->extension_state = pn_data(0);
    }
    pn_data_rewind(delivery->extension_state);
    return delivery->extension_state;
}

void qdr_delivery_free_extension_state(qdr_delivery_t *delivery)
{
    if (delivery->extension_state) {
        pn_data_free(delivery->extension_state);
        delivery->extension_state = 0;
    }
}

void qdr_delivery_write_extension_state(qdr_delivery_t *dlv, pn_delivery_t* pdlv)
{
    if (dlv->disposition > PN_MODIFIED) {
        pn_data_copy(pn_disposition_data(pn_delivery_local(pdlv)), qdr_delivery_extension_state(dlv));
        qdr_delivery_free_extension_state(dlv);
    }
}


void qdr_delivery_move_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest)
{
    if (src->disposition > PN_MODIFIED) {
        pn_data_copy(qdr_delivery_extension_state(dest), qdr_delivery_extension_state(src));
        qdr_delivery_free_extension_state(src);
    }
}


void qdr_delivery_copy_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest)
{
    if (src->disposition > PN_MODIFIED) {
        pn_data_copy(qdr_delivery_extension_state(dest), qdr_delivery_extension_state(src));
    }
}


void qdr_delivery_read_extension_state(qdr_delivery_t *dlv, uint64_t disposition, pn_data_t* disposition_data)
{
    if (disposition > PN_MODIFIED) {
        pn_data_rewind(disposition_data);
        pn_data_copy(qdr_delivery_extension_state(dlv), disposition_data);
    }
}


/**
 * The delivery's link has gone down.
 *
 * Update all of dlv's 'peer' deliveries properly based on their state. If dlv
 * is outgoing the release flag determines if the corresponding inbound
 * delivery can be released.
 */
void qdr_delivery_link_dropped_CT(qdr_core_t *core, qdr_delivery_t *dlv, bool release)
{
    if (dlv->incoming) {

        if (!qdr_delivery_receive_complete(dlv)) {
            qdr_delivery_set_aborted(dlv, true);
            qdr_delivery_continue_transfer(core, dlv);
        }

        // fake a settle to all outbound deliveries in case remote expects
        // settlement

        qdr_delivery_lock_transfer(dlv);

        qdr_delivery_in_t *in_dlv = (qdr_delivery_in_t *)dlv;
        in_dlv->base.settled = true;

        qdr_delivery_out_t *out_dlv = DEQ_HEAD(in_dlv->out_dlvs);
        while (out_dlv) {
            out_dlv->base.settled = true;
            if (qdr_delivery_settled_CT(core, &out_dlv->base))
                qdr_delivery_push_CT(core, &out_dlv->base);
            qdr_delivery_unlink_peers_CT(core, dlv, &out_dlv->base);
            out_dlv = DEQ_HEAD(in_dlv->out_dlvs);
        }

        qdr_delivery_unlock_transfer(dlv);

        qdr_delivery_increment_counters_CT(core, dlv);
        qd_nullify_safe_ptr(&dlv->link_sp);


    } else {    // dlv is outgoing

        qdr_delivery_lock_transfer(dlv);

        qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)dlv;
        qdr_delivery_in_t  *in_dlv  = out_dlv->in_dlv;

        if (in_dlv) {
            // only update if last outstanding delivery
            if (DEQ_SIZE(in_dlv->out_dlvs) == 1) {
                if (release)
                    qdr_delivery_release_CT(core, &in_dlv->base);
                else
                    qdr_delivery_failed_CT(core, &in_dlv->base);
            }
            qdr_delivery_unlink_peers_CT(core, &in_dlv->base, &out_dlv->base);
        }

        qdr_delivery_unlock_transfer(dlv);

        qdr_delivery_increment_counters_CT(core, dlv);
        qd_nullify_safe_ptr(&dlv->link_sp);
    }
}

void qdr_delivery_lock_transfer(qdr_delivery_t *delivery)
{
    if (delivery)
        qd_message_lock(delivery->msg);
}


void qdr_delivery_unlock_transfer(qdr_delivery_t *delivery)
{
    if (delivery)
        qd_message_unlock(delivery->msg);
}


// Called when the core has completed forwarding and there are no in-core
// consumers (subscriptions or endpoints)
//
void qdr_delivery_forwarding_done_CT(qdr_core_t *core, qdr_delivery_t *in_delivery)
{
    if (in_delivery) {
        assert(in_delivery->incoming);
        ((qdr_delivery_in_t *)in_delivery)->send_to_core = false;
    }
}


// Called from I/O thread during updated delivery processing.  The state of the
// qdr_delivery_t has been updated by either core or a peer delivery and it
// must be synchronized with its corresponding pn_delivery_t.
//
void qdr_delivery_do_update(qdr_core_t *core, qdr_delivery_t *delivery)
{
    bool can_settle = false;
    bool discard = false;
    qdr_delivery_t *peer = 0;
    bool decref = false;
    uint64_t disp = 0;

    // note: qdr_delivery_update_peer_LH will set both settled and
    // disposition under lock.  We need to be sure that we're consistent
    // otherwise we may settle before the disposition change becomes
    // visible
    qdr_delivery_lock_transfer(delivery);

    disp = delivery->disposition;

    if (delivery->settled) {
        // in general we don't settle locally until this delivery's I/O is complete,
        if (delivery->incoming) {
            if (qdr_delivery_receive_complete(delivery)) {
                can_settle = true;
            } else if (DEQ_SIZE(((qdr_delivery_in_t *)delivery)->out_dlvs) == 0
                       && !((qdr_delivery_in_t *)delivery)->send_to_core) {
                // however if there are no consumers left we must discard the message
                discard = true;
            }

        } else if (qdr_delivery_send_complete(delivery)) {  // outgoing delivery
            can_settle = true;

            // can now unlink delivery from its incoming peer

            qdr_delivery_out_t *out_dlv = (qdr_delivery_out_t *)delivery;
            qdr_delivery_in_t *in_dlv = out_dlv->in_dlv;
            if (in_dlv) {
                peer = &in_dlv->base;
                out_dlv->in_dlv = 0;
                DEQ_REMOVE(in_dlv->out_dlvs, out_dlv);
                decref = true;
            }
        }
    }

    qdr_delivery_unlock_transfer(delivery);

    bool settled = core->delivery_update_handler(core->user_context, delivery, disp, can_settle, discard);

    if (settled) {
        // notify core...
        qdr_action_t *action = qdr_action(qdr_delivery_update_CT, "update_delivery");
        action->args.delivery.delivery = delivery;
        qdr_action_enqueue(core, action);
    }

    if (decref) {
        qdr_delivery_decref(core, delivery, "qdr_delivery_do_update - out dlv reference drop");
        qdr_delivery_decref(core, peer, "qdr_delivery_do_update - in dlv settled & send complete");
    }
}


bool qdr_delivery_do_deliver(qdr_core_t *core, qdr_link_t *link, qdr_delivery_t *out_dlv)
{

    assert(!out_dlv->incoming);

    bool remote_settle = core->deliver_handler(core->user_context, link, out_dlv);
    bool send_complete = qdr_delivery_send_complete(out_dlv);

    if (send_complete) {
        bool decref = false;
        bool update_peer = false;

        qdr_delivery_lock_transfer(out_dlv);

        // if the remote requires send_settle or we aborted it is treated as if
        // the remote has updated the disposition and settled. If this happens
        // we need to update the incoming peer delivery
        if (!out_dlv->settled && (remote_settle || qd_message_aborted(out_dlv->msg))) {
            out_dlv->settled = true;
            update_peer = true;
        }
        if (remote_settle && !out_dlv->disposition) {
            out_dlv->disposition = PN_ACCEPTED;
            update_peer = true;
        }

        bool settled = out_dlv->settled;
        uint64_t disp = out_dlv->disposition;

        qdr_delivery_in_t *peer = ((qdr_delivery_out_t *)out_dlv)->in_dlv;
        if (peer) {
            if (update_peer && DEQ_SIZE(peer->out_dlvs) == 1) {
                // @TODO(kgiusti) - need copy of error?
                qdr_delivery_update_peer_LH(&peer->base, disp, settled, qdr_delivery_extension_state(out_dlv), 0);
            }

            if (settled) {
                DEQ_REMOVE(peer->out_dlvs, (qdr_delivery_out_t *)out_dlv);
                ((qdr_delivery_out_t *)out_dlv)->in_dlv = 0;
                decref = true;
            }
        }

        qdr_delivery_unlock_transfer(out_dlv);

        settled = core->delivery_update_handler(core->user_context, out_dlv, disp, settled, false);
        if (settled) {
            // notify core
            qdr_action_t *action = qdr_action(qdr_delivery_update_CT, "update_delivery");
            action->args.delivery.delivery = out_dlv;
            qdr_action_enqueue(core, action);
        }

        if (decref) {
            qdr_delivery_decref(core, &peer->base, "qdr_delivery_do_deliver - drop ref to peer in dlv");
            qdr_delivery_decref(core, out_dlv, "qdr_delivery_do_deliver - out_dlv remote settled");
        }
    }

    return send_complete;
}
