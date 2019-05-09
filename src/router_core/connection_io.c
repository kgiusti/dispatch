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


//
// Connection processing code that runs on an I/O thread
//


#include <qpid/dispatch/router_core.h>
#include "router_core_private.h"
#include "delivery_io.h"


int qdr_connection_process(qdr_connection_t *conn)
{
    qdr_connection_work_list_t  work_list;
    qdr_link_ref_list_t         links_with_work[QDR_N_PRIORITIES];
    qdr_core_t                 *core = conn->core;

    qdr_link_ref_t *ref;
    qdr_link_t     *link;
    bool            detach_sent;

    int event_count = 0;

    if (conn->closed) {
        core->conn_close_handler(core->user_context, conn, conn->error);
        return 0;
    }

    sys_mutex_lock(conn->work_lock);
    DEQ_MOVE(conn->work_list, work_list);
    for (int priority = 0; priority <= QDR_MAX_PRIORITY; ++ priority) {
        DEQ_MOVE(conn->links_with_work[priority], links_with_work[priority]);

        //
        // Move the references from CLASS_WORK to CLASS_LOCAL so concurrent action in the core
        // thread doesn't assume these links are referenced from the connection's list.
        //
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            move_link_ref(ref->link, QDR_LINK_LIST_CLASS_WORK, QDR_LINK_LIST_CLASS_LOCAL);
            ref->link->processing = true;
            ref = DEQ_NEXT(ref);
        }
    }
    sys_mutex_unlock(conn->work_lock);

    event_count += DEQ_SIZE(work_list);
    qdr_connection_work_t *work = DEQ_HEAD(work_list);
    while (work) {
        DEQ_REMOVE_HEAD(work_list);

        switch (work->work_type) {
        case QDR_CONNECTION_WORK_FIRST_ATTACH :
            core->first_attach_handler(core->user_context, conn, work->link, work->source, work->target);
            break;

        case QDR_CONNECTION_WORK_SECOND_ATTACH :
            core->second_attach_handler(core->user_context, work->link, work->source, work->target);
            break;
        }

        qdr_connection_work_free_CT(work);
        work = DEQ_HEAD(work_list);
    }

    // Process the links_with_work array from highest to lowest priority.
    for (int priority = QDR_MAX_PRIORITY; priority >= 0; -- priority) {
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            qdr_link_work_t *link_work;
            detach_sent = false;
            link = ref->link;

            //
            // The work lock must be used to protect accesses to the link's work_list and
            // link_work->processing.
            //
            sys_mutex_lock(conn->work_lock);
            link_work = DEQ_HEAD(link->work_list);
            if (link_work) {
                DEQ_REMOVE_HEAD(link->work_list);
                link_work->processing = true;
            }
            sys_mutex_unlock(conn->work_lock);

            //
            // Handle disposition/settlement updates
            //
            qdr_delivery_ref_list_t updated_deliveries;
            sys_mutex_lock(conn->work_lock);
            DEQ_MOVE(link->updated_deliveries, updated_deliveries);
            sys_mutex_unlock(conn->work_lock);

            qdr_delivery_ref_t *dref = DEQ_HEAD(updated_deliveries);
            while (dref) {
                qdr_delivery_do_update(core, dref->dlv);
                qdr_delivery_decref(core, dref->dlv, "qdr_connection_process - remove from updated list");
                qdr_del_delivery_ref(&updated_deliveries, dref);
                dref = DEQ_HEAD(updated_deliveries);
                event_count++;
            }

            while (link_work) {
                switch (link_work->work_type) {
                case QDR_LINK_WORK_DELIVERY :
                    {
                        int count = core->push_handler(core->user_context, link, link_work->value);
                        assert(count <= link_work->value);
                        link_work->value -= count;
                        break;
                    }

                case QDR_LINK_WORK_FLOW :
                    if (link_work->value > 0)
                        core->flow_handler(core->user_context, link, link_work->value);
                    if      (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_SET)
                        core->drain_handler(core->user_context, link, true);
                    else if (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_CLEAR)
                        core->drain_handler(core->user_context, link, false);
                    else if (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_DRAINED)
                        core->drained_handler(core->user_context, link);
                    break;

                case QDR_LINK_WORK_FIRST_DETACH :
                case QDR_LINK_WORK_SECOND_DETACH :
                    core->detach_handler(core->user_context, link, link_work->error,
                                         link_work->work_type == QDR_LINK_WORK_FIRST_DETACH,
                                         link_work->close_link);
                    detach_sent = true;
                    break;
                }

                sys_mutex_lock(conn->work_lock);
                if (link_work->work_type == QDR_LINK_WORK_DELIVERY && link_work->value > 0 && !link->detach_received) {
                    DEQ_INSERT_HEAD(link->work_list, link_work);
                    link_work->processing = false;
                    link_work = 0; // Halt work processing
                } else {
                    qdr_error_free(link_work->error);
                    free_qdr_link_work_t(link_work);
                    link_work = DEQ_HEAD(link->work_list);
                    if (link_work) {
                        DEQ_REMOVE_HEAD(link->work_list);
                        link_work->processing = true;
                    }
                }
                sys_mutex_unlock(conn->work_lock);
                event_count++;
            }

            if (detach_sent) {
                // let the core thread know so it can clean up
                qdr_link_detach_sent(link);
            }

            ref = DEQ_NEXT(ref);
        }
    }

    sys_mutex_lock(conn->work_lock);
    for (int priority = QDR_MAX_PRIORITY; priority >= 0; -- priority) {
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            qdr_link_t *link = ref->link;

            link->processing = false;
            if (link->ready_to_free)
                qdr_link_processing_complete(core, link);

            qdr_del_link_ref(links_with_work + priority, ref->link, QDR_LINK_LIST_CLASS_LOCAL);
            ref = DEQ_HEAD(links_with_work[priority]);
        }
    }
    sys_mutex_unlock(conn->work_lock);

    return event_count;
}
