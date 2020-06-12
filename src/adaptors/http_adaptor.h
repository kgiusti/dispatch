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
#include <qpid/dispatch/server.h>
#include <qpid/dispatch/threading.h>
#include <qpid/dispatch/compose.h>
#include <qpid/dispatch/atomic.h>
#include <qpid/dispatch/alloc.h>
#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/log.h>
#include <nghttp2/nghttp2.h>

// We already have a qd_http_listener_t defined in http-libwebsockets.c
// We will call this as qd_http_lsnr_t in order to avoid a clash.
// At a later point in time, we will handle websocket here as well
// and get rid of http-libwebsockets.c and rename this as qd_http_listener_t
typedef struct qd_http_lsnr_t          qd_http_lsnr_t;
typedef struct qd_http_connector_t     qd_http_connector_t;
typedef struct qd_http2_session_data_t qd_http2_session_data_t;
typedef struct qd_bridge_config_t      qd_bridge_config_t;
typedef struct qd_http2_stream_data_t  qd_http2_stream_data_t;
typedef struct qdr_http_connection_t  qdr_http_connection_t;
//typedef struct qd_http2_user_context_t  qd_http2_user_context_t;

struct qd_bridge_config_t {
    char *name;
    char *host;
    char *port;
    char *address;
    char *host_port;
};

struct qd_http_lsnr_t {
    qd_handler_context_t       context;
    sys_atomic_t               ref_count;
    qd_server_t               *server;
    qd_bridge_config_t         config;
    pn_listener_t             *pn_listener;
    DEQ_LINKS(qd_http_lsnr_t);
};

struct qd_http_connector_t {
    sys_atomic_t              ref_count;
    qd_server_t              *server;
    qd_bridge_config_t        config;
    qd_timer_t               *timer;
    long                      delay;

    DEQ_LINKS(qd_http_connector_t);
};

struct qd_http2_stream_data_t {
    char *request_path;
    int32_t stream_id;

    //Client data
    const char *uri; // The NULL-terminated URI string to retrieve.
    /* The authority portion of the |uri|, not NULL-terminated */
    char *authority;
    /* The path portion of the |uri|, including query, not NULL-terminated */
    char *path;
    /* The length of the |authority| */
    size_t authoritylen;
    /* The length of the |path| */
    size_t pathlen;
    DEQ_LINKS(qd_http2_stream_data_t);
};

struct qdr_http_connection_t {
    qd_handler_context_t  context;
    char                 *reply_to;
    qdr_connection_t     *qdr_conn;
    qdr_link_t           *in_link;
    qdr_link_t           *out_link;
    uint64_t              incoming_id;
    uint64_t              outgoing_id;
    pn_raw_connection_t  *pn_raw_conn;
    pn_raw_buffer_t       read_buffers[4];
    qdr_delivery_t       *in_dlv;
    qdr_delivery_t       *out_dlv;
    bool                  ingress;
    qd_timer_t           *activate_timer;
    qd_bridge_config_t   *config;
    qd_server_t          *server;
    uint64_t              conn_id;
    qd_http2_session_data_t *session_data;
    char                 *remote_address;
};

//struct qd_http2_user_context_t {
//    qd_message_t *message;
//};




DEQ_DECLARE(qd_http_lsnr_t, qd_http_lsnr_list_t);
ALLOC_DECLARE(qd_http_lsnr_t);
ALLOC_DECLARE(qd_http2_session_data_t);
ALLOC_DECLARE(qd_http_connector_t);
ALLOC_DECLARE(qd_http2_stream_data_t);
DEQ_DECLARE(qd_http_connector_t, qd_http_connector_list_t);
DEQ_DECLARE(qd_http2_stream_data_t, qd_http2_stream_data_list_t);

struct qd_http2_session_data_t {
    qd_http2_stream_data_list_t  streams;
    int                          num_headers;

    qd_http2_stream_data_t      *stream_data; // This field is temporary. We will remove it.
    qd_message_t                *message;
    nghttp2_session             *session;
    char                        *client_addr;
    char                        *path;   // This field is mapped to the 'to' field of AMQP
    char                        *method; // HTTP method - GET, POST, HEAD etc.
    char                        *content_type; //content_type.
    char                        *content_encoding;
    char                        *status;  // Response status like 200:OK
    qdr_http_connection_t       *conn;
    size_t                       request_header_len;
    size_t                       response_header_len;
    bool                         has_data;  // Did we ever receive a request or a response DATA frame.

    //Might need to create a request data field and move these there
    qd_composed_field_t         *header_properties;  // This has the header and the properties.
    qd_composed_field_t         *app_properties;     // This has the application properties.
    bool                  header_sent;
    qd_buffer_list_t            buff_list;



    // A linked list of buffers that contain data that need to be written outbound
    qd_buffer_list_t             out_buffs;
    qd_iterator_pointer_t        cursor;
    qd_iterator_pointer_t        sent_cursor;


};
