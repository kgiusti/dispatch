#ifndef __delivery_io_h__
#define __delivery_io_h__ 1
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
// Delivery API for I/O threads
//


#include <qpid/dispatch/delivery.h>

/* release dlv and possibly schedule its deletion on the core thread */
void qdr_delivery_decref(qdr_core_t *core, qdr_delivery_t *delivery, const char *label);

/* handles disposition/settlement changes from remote delivery and schedules Core thread */
bool qdr_delivery_update_disposition(qdr_core_t *core, qdr_delivery_t *delivery, uint64_t disp,
                                     bool settled, qdr_error_t *error, pn_data_t *ext_state);

/* invoked when more incoming message data arrives - schedule outbound
 * deliveries and possibly schedule core thread */
void qdr_deliver_continue(qdr_core_t *core, qdr_delivery_t *in_dlv);

/* update the proton delivery's state to match delivery */
void qdr_delivery_do_update(qdr_core_t *core, qdr_delivery_t *delivery);

/* send out_dlv message data out link */
bool qdr_delivery_do_deliver(qdr_core_t *core, qdr_link_t *link, qdr_delivery_t *out_dlv);

/* write extension state data to a proton delivery */
void qdr_delivery_write_extension_state(qdr_delivery_t *dlv, pn_delivery_t* pdlv);






#endif // __delivery_io_h__
