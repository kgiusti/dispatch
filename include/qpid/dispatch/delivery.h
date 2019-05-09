#ifndef __delivery_h__
#define __delivery_h__ 1
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
// Defines the part of the delivery API that is thread agnostic - these methods
// can be invoked from any thread.
//

#include <qpid/dispatch/router_core.h>

#define QDR_DELIVERY_TAG_MAX 32

typedef enum {
    QDR_DELIVERY_NOWHERE = 0,
    QDR_DELIVERY_IN_UNDELIVERED,
    QDR_DELIVERY_IN_UNSETTLED,
} qdr_delivery_where_t;


qdr_delivery_t *qdr_delivery(qdr_link_t *link);

bool qdr_delivery_receive_complete(const qdr_delivery_t *delivery);
bool qdr_delivery_send_complete(const qdr_delivery_t *delivery);

void qdr_delivery_set_context(qdr_delivery_t *delivery, void *context);
void *qdr_delivery_get_context(const qdr_delivery_t *delivery);

bool qdr_delivery_is_settled(const qdr_delivery_t *dlv);

qd_message_t *qdr_delivery_message(const qdr_delivery_t *delivery);
qdr_error_t *qdr_delivery_error(const qdr_delivery_t *delivery);
qdr_link_t *qdr_delivery_link(const qdr_delivery_t *delivery);

bool qdr_delivery_is_presettled(const qdr_delivery_t *delivery);

uint64_t qdr_delivery_disposition(const qdr_delivery_t *delivery);
void qdr_delivery_set_disposition(qdr_delivery_t *delivery, uint64_t disposition);

void qdr_delivery_set_aborted(qdr_delivery_t *delivery, bool aborted);
bool qdr_delivery_is_aborted(const qdr_delivery_t *delivery);

void qdr_delivery_tag(const qdr_delivery_t *delivery, const char **tag, int *length);
bool qdr_delivery_tag_sent(const qdr_delivery_t *delivery);
void qdr_delivery_set_tag_sent(const qdr_delivery_t *delivery, bool tag_sent);

void qdr_delivery_incref(qdr_delivery_t *delivery, const char *label);




#endif // __delivery_h__
