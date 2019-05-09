#ifndef __delivery_core_h__
#define __delivery_core_h__ 1

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
#include "router_core_private.h"

struct qdr_delivery_t {
    DEQ_LINKS(qdr_delivery_t);
    void                   *context;
    sys_atomic_t            ref_count;
    bool                    ref_counted;   /// Used to protect against ref count going 1 -> 0 -> 1
    qdr_link_t_sp           link_sp;       /// Safe pointer to the link
    qd_message_t           *msg;
    qd_iterator_t          *to_addr;
    qd_iterator_t          *origin;
    uint64_t                disposition;   // set by owning I/O thread to current pn_delivery_t dispo value
    uint32_t                ingress_time;
    pn_data_t              *extension_state;
    qdr_error_t            *error;
    bool                    settled;       // set true by owning I/O thread when local pn_delivery_t settled
    bool                    presettled;
    bool                    incoming;
    qdr_delivery_where_t    where;
    uint8_t                 tag[QDR_DELIVERY_TAG_MAX];
    int                     tag_length;
    qd_bitmask_t           *link_exclusion;
    qdr_address_t          *tracking_addr;
    int                     tracking_addr_bit;
    int                     ingress_index;
    qdr_link_work_t        *link_work;         ///< Delivery work item for this delivery
    qdr_subscription_list_t subscriptions;
    bool                    multicast;         /// True if this delivery is targeted for a multicast address.
    bool                    via_edge;          /// True if this delivery arrived via an edge-connection.
};


// used to string together a list of outgoing multicast deliveries
//
typedef struct qdr_delivery_mcast_node_t {
    DEQ_LINKS(struct qdr_delivery_mcast_node_t);
    qdr_link_t     *out_link;
    qdr_delivery_t *out_dlv;
} qdr_delivery_mcast_node_t;
DEQ_DECLARE(qdr_delivery_mcast_node_t, qdr_delivery_mcast_list_t);
ALLOC_DECLARE(qdr_delivery_mcast_node_t);


///////////////////////////////////////////////////////////////////////////////
//                               Delivery API
///////////////////////////////////////////////////////////////////////////////


/* copy extension state data from proton into a delivery */
void qdr_delivery_read_extension_state(qdr_delivery_t *dlv, uint64_t disposition, pn_data_t* disposition_data);

/* transfer extension state data between deliveries */
void qdr_delivery_move_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest);

/* copy src extension state info dest */
void qdr_delivery_copy_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest);


//
// CORE thread only functions
//


/* update settlement and/or disposition and schedule I/O processing */
void qdr_delivery_release_CT(qdr_core_t *core, qdr_delivery_t *delivery);
void qdr_delivery_failed_CT(qdr_core_t *core, qdr_delivery_t *delivery);
bool qdr_delivery_settled_CT(qdr_core_t *core, qdr_delivery_t *delivery);

/* add dlv to links list of updated deliveries and schedule I/O thread processing */
void qdr_delivery_push_CT(qdr_core_t *core, qdr_delivery_t *dlv);

/* optimized decref for core thread */
void qdr_delivery_decref_CT(qdr_core_t *core, qdr_delivery_t *delivery, const char *label);

// set in_dlv's outgoing peer delivery
bool qdr_delivery_set_outgoing_CT(qdr_core_t *core, qdr_delivery_t *in_dlv, qdr_delivery_t *out_dlv);

// set in_dlv's list of outgoing mcast deliveries
void qdr_delivery_set_mcasts_CT(qdr_core_t *core, qdr_delivery_t *in_dlv, const qdr_delivery_mcast_list_t *out_dlvs);

// the delivery's link is gone - update peer deliveries and cleanup link related stuff
void qdr_delivery_link_dropped_CT(qdr_core_t *core, qdr_delivery_t *dlv, bool release);

// called when the router has completed the forwarding process
void qdr_delivery_forwarding_done_CT(qdr_core_t *core, qdr_delivery_t *in_delivery);

#endif // __delivery_core_h__
