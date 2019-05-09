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

#include <qpid/dispatch/router_core.h>
#include "router_core_private.h"
#include "delivery_io.h"
#include "delivery_core.h"

//
// I/O thread processing for outbound delivery data
//

int qdr_link_process_deliveries(qdr_core_t *core, qdr_link_t *link, int credit)
{
    qdr_connection_t *conn = link->conn;
    qdr_delivery_t   *dlv;
    int               offer   = -1;
    bool              settled = false;
    bool              send_complete = false;
    int               num_deliveries_completed = 0;

    if (link->link_direction == QD_OUTGOING) {

        // If a detach has been received on the link, there is no need to process deliveries on the link.
        if (link->detach_received)
            return 0;

        while (credit > 0) {
            sys_mutex_lock(conn->work_lock);
            dlv = DEQ_HEAD(link->undelivered);
            if (dlv) {
                qdr_delivery_incref(dlv, "qdr_link_process_deliveries - holding the undelivered delivery locally");

                sys_mutex_unlock(conn->work_lock);
                send_complete = qdr_delivery_do_deliver(core, link, dlv);
                sys_mutex_lock(conn->work_lock);

                if (send_complete) {
                    //
                    // The entire message has been sent. It is now the appropriate time to have the delivery removed
                    // from the head of the undelivered list and move it to the unsettled list if it is not settled.
                    //
                    num_deliveries_completed++;

                    credit--;
                    link->credit_to_core--;
                    link->total_deliveries++;

                    // DISPATCH-1153:
                    // If the undelivered list is cleared the link may have detached.  Stop processing.
                    offer = DEQ_SIZE(link->undelivered);
                    if (offer == 0) {
                        qdr_delivery_decref(core, dlv, "qdr_link_process_deliveries - release local reference - closed link");
                        sys_mutex_unlock(conn->work_lock);
                        return num_deliveries_completed;
                    }

                    assert(dlv == DEQ_HEAD(link->undelivered));
                    DEQ_REMOVE_HEAD(link->undelivered);
                    dlv->link_work = 0;

                    if (settled) {
                        dlv->where = QDR_DELIVERY_NOWHERE;
                        qdr_delivery_decref(core, dlv, "qdr_link_process_deliveries - remove from undelivered list");
                    } else {
                        DEQ_INSERT_TAIL(link->unsettled, dlv);
                        dlv->where = QDR_DELIVERY_IN_UNSETTLED;
                        qd_log(core->log, QD_LOG_DEBUG, "Delivery transfer:  dlv:%lx qdr_link_process_deliveries: undelivered-list -> unsettled-list", (long) dlv);
                    }
                }
                else {
                    qdr_delivery_decref(core, dlv, "qdr_link_process_deliveries - release local reference - not send_complete");

                    //
                    // The message is still being received/sent.
                    // 1. We cannot remove the delivery from the undelivered list.
                    //    This delivery needs to stay at the head of the undelivered list until the entire message
                    //    has been sent out i.e other deliveries in the undelivered list have to wait before this
                    //    entire large delivery is sent out
                    // 2. We need to call deliver_handler so any newly arrived bytes can be pushed out
                    // 3. We need to break out of this loop otherwise a thread will keep spinning in here until
                    //    the entire message has been sent out.
                    //
                    sys_mutex_unlock(conn->work_lock);

                    //
                    // Note here that we are not incrementing num_deliveries_processed. Since this delivery is
                    // still coming in or still being sent out, we cannot consider this delivery as fully processed.
                    //
                    return num_deliveries_completed;
                }
                sys_mutex_unlock(conn->work_lock);

                qdr_delivery_decref(core, dlv, "qdr_link_process_deliveries - release local reference - done processing");
            } else {
                sys_mutex_unlock(conn->work_lock);
                break;
            }
        }

        if (offer != -1)
            core->offer_handler(core->user_context, link, offer);
    }

    return num_deliveries_completed;
}
