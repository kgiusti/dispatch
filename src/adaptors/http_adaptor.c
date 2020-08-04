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
#include <stdio.h>
#include <inttypes.h>

#include <proton/condition.h>
#include <proton/listener.h>
#include <proton/proactor.h>
#include <proton/netaddr.h>
#include <proton/raw_connection.h>
#include <nghttp2/nghttp2.h>

#include <qpid/dispatch/buffer.h>

#include "qpid/dispatch/protocol_adaptor.h"
#include "delivery.h"
#include "http_common.h"
#include "http_adaptor.h"

const char *PATH = ":path";
const char *METHOD = ":method";
const char *STATUS = ":status";
const char *CONTENT_TYPE = "content-type";
const char *CONTENT_ENCODING = "content-encoding";

#define READ_BUFFERS 4
#define WRITE_BUFFERS 4
#define DEBUGBUILD 1
#define ARRLEN(x) (sizeof(x) / sizeof(x[0]))

#define MAKE_NV2(NAME, VALUE)                                                  \
{                                                                              \
    (uint8_t *)NAME, (uint8_t *)VALUE, sizeof(NAME) - 1, sizeof(VALUE) - 1,    \
        NGHTTP2_NV_FLAG_NONE                                                   \
}

ALLOC_DEFINE(qd_http_lsnr_t);
ALLOC_DEFINE(qd_http_connector_t);
ALLOC_DEFINE(qdr_http2_session_data_t);
ALLOC_DEFINE(qdr_http2_stream_data_t);

typedef struct qdr_http_adaptor_t {
    qdr_core_t              *core;
    qdr_protocol_adaptor_t  *adaptor;
    qd_http_lsnr_list_t      listeners;
    qd_http_connector_list_t connectors;
    qd_log_source_t         *log_source;
    void                    *callbacks;
    qd_log_source_t         *protocol_log_source;
} qdr_http_adaptor_t;


static qdr_http_adaptor_t *http_adaptor;

static void handle_connection_event(pn_event_t *e, qd_server_t *qd_server, void *context);

/**
 * HTTP :path is mapped to the AMQP 'to' field.
 */
qd_composed_field_t  *qd_message_compose_amqp(qd_message_t *msg,
                                              const char *to,
                                              const char *subject,
                                              const char *reply_to,
                                              const char *content_type,
                                              const char *content_encoding,
                                              int32_t  correlation_id)
{
    qd_composed_field_t  *field   = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_message_content_t *content = MSG_CONTENT(msg);
    if (!content)
        return 0;
    //
    // Header
    //
    qd_compose_start_list(field);
    qd_compose_insert_bool(field, 0);     // durable
    qd_compose_insert_null(field);        // priority
    //qd_compose_insert_null(field);        // ttl
    //qd_compose_insert_bool(field, 0);     // first-acquirer
    //qd_compose_insert_uint(field, 0);     // delivery-count
    qd_compose_end_list(field);

    //
    // Properties
    //
    field = qd_compose(QD_PERFORMATIVE_PROPERTIES, field);
    qd_compose_start_list(field);
    qd_compose_insert_null(field);          // message-id
    qd_compose_insert_null(field);          // user-id
    if (to) {
        qd_compose_insert_string(field, to);    // to
    }
    else {
        qd_compose_insert_null(field);
    }

    if (subject) {
        qd_compose_insert_string(field, subject);      // subject
    }
    else {
        qd_compose_insert_null(field);
    }

    if (reply_to) {
        qd_compose_insert_string(field, reply_to); // reply-to
    }
    else {
        qd_compose_insert_null(field);
    }

    if (correlation_id > 0) {
        qd_compose_insert_int(field, correlation_id);
    }
    else {
        qd_compose_insert_null(field);          // correlation-id
    }

    if (content_type) {
        qd_compose_insert_string(field, content_type);        // content-type
    }
    else {
        qd_compose_insert_null(field);
    }
    if (content_encoding) {
        qd_compose_insert_string(field, content_encoding);               // content-encoding
    }
    else {
        qd_compose_insert_null(field);
    }
    qd_compose_end_list(field);

    return field;
}

static char *get_address_string(pn_raw_connection_t *pn_raw_conn)
{
    const pn_netaddr_t *netaddr = pn_raw_connection_remote_addr(pn_raw_conn);
    char buffer[1024];
    int len = pn_netaddr_str(netaddr, buffer, 1024);
    if (len <= 1024) {
        return strdup(buffer);
    } else {
        return strndup(buffer, 1024);
    }
}

void free_qdr_http_connection(qdr_http_connection_t* http_conn)
{
    if(http_conn->remote_address) {
        free(http_conn->remote_address);
    }
    if (http_conn->activate_timer) {
        qd_timer_free(http_conn->activate_timer);
    }
    nghttp2_session_del(http_conn->session_data->session);
    free(http_conn);
}

static qdr_http2_stream_data_t *create_http2_stream_data(qdr_http2_session_data_t *session_data, int32_t stream_id)
{
    qdr_http2_stream_data_t *stream_data = new_qdr_http2_stream_data_t();
    ZERO(stream_data);
    stream_data->stream_id = stream_id;
    stream_data->message = qd_message();
    stream_data->session_data = session_data;
    nghttp2_session_set_stream_user_data(session_data->session, stream_id, stream_data);
    DEQ_INSERT_TAIL(session_data->streams, stream_data);
    return stream_data;
}

void free_http2_stream_data(qdr_http2_session_data_t *session_data, int32_t stream_id)
{
    qdr_http2_stream_data_t *stream_data = DEQ_HEAD(session_data->streams);
    while (stream_data) {
        if (stream_data->stream_id == stream_id) {
            DEQ_REMOVE(session_data->streams, stream_data);
            nghttp2_session_set_stream_user_data(session_data->session, stream_id, NULL);

            //
            // TODO - Free all stream related data
            //
            qdr_link_detach(stream_data->in_link, QD_CLOSED, 0);
            qdr_link_detach(stream_data->out_link, QD_CLOSED, 0);
            free(stream_data->reply_to);
            qd_compose_free(stream_data->app_properties);
            free_qdr_http2_stream_data_t(stream_data);
            break;
        }
    }
}


static int on_data_chunk_recv_callback(nghttp2_session *session,
                                                   uint8_t flags,
                                                   int32_t stream_id,
                                                   const uint8_t *data,
                                                   size_t len, void *user_data)
{
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(session_data->session, stream_id);

    qd_buffer_list_t buffers;
    DEQ_INIT(buffers);
    qd_buffer_list_append(&buffers, (uint8_t *)data, len);

    qd_composed_field_t *body = qd_compose(QD_PERFORMATIVE_BODY_DATA, 0);
    qd_compose_insert_buffers(body, &buffers);

    qd_message_extend(stream_data->message, body);
    nghttp2_session_consume(session, stream_id, len);
    qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 on_data_chunk_recv_callback data length %zu", conn->conn_id, stream_id, len);
    return 0;
}

static int on_stream_close_callback(nghttp2_session *session,
                                    int32_t stream_id,
                                    nghttp2_error_code error_code,
                                    void *user_data)
{
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;
    qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 on_stream_close_callback ", conn->conn_id, stream_id);
    free_http2_stream_data(session_data, stream_id);
    return 0;
}

/* nghttp2_send_callback. The data pointer passed into this function contains encoded HTTP data. Here we transmit the |data|, |length| bytes,
   to the network. */
static ssize_t send_callback(nghttp2_session *session,
                             const uint8_t *data,
                             size_t length,
                             int flags,
                             void *user_data) {
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;
    qd_buffer_list_append(&(session_data->buffs), (uint8_t *)data, length);
    qd_log(http_adaptor->protocol_log_source, QD_LOG_TRACE, "[C%i] HTTP2 send_callback data length %zu", conn->conn_id, length);
    return (ssize_t)length;
}

/**
 * This callback function is invoked with the reception of header block in HEADERS or PUSH_PROMISE is started
 */
static int on_begin_headers_callback(nghttp2_session *session,
                                     const nghttp2_frame *frame,
                                     void *user_data)
{
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;

    if (frame->hd.type == NGHTTP2_HEADERS) {
        if(frame->headers.cat == NGHTTP2_HCAT_REQUEST && conn->ingress) {
            // This is a brand new request.
            int32_t stream_id = frame->hd.stream_id;
            qdr_terminus_t *target = qdr_terminus(0);
            qdr_http2_stream_data_t *stream_data = create_http2_stream_data(session_data, stream_id);
            qd_log(http_adaptor->protocol_log_source, QD_LOG_TRACE, "[C%i] Processing incoming HTTP2 stream with id %"PRId32"", conn->conn_id, stream_id);
            //
            // For every stream create  -
            // 1. sending link with the configured address as the target
            //
            qdr_terminus_set_address(target, conn->config->address);
            stream_data->in_link = qdr_link_first_attach(conn->qdr_conn,
                                                         QD_INCOMING,
                                                         qdr_terminus(0),  //qdr_terminus_t   *source,
                                                         target,           //qdr_terminus_t   *target,
                                                         "tcp.ingress.in",         //const char       *name,
                                                         0,                //const char       *terminus_addr,
                                                         false,
                                                         NULL,
                                                         &(stream_data->incoming_id));
            qdr_link_set_context(stream_data->in_link, stream_data);

            //
            // 2. dynamic receiver on which to receive back the response data for that stream
            //
            qdr_terminus_t *dynamic_source = qdr_terminus(0);
            qdr_terminus_set_dynamic(dynamic_source);
            stream_data->out_link = qdr_link_first_attach(conn->qdr_conn,
                                                          QD_OUTGOING,   //Receiver
                                                          dynamic_source,   //qdr_terminus_t   *source,
                                                          qdr_terminus(0),  //qdr_terminus_t   *target,
                                                          "http.ingress.out",        //const char       *name,
                                                          0,                //const char       *terminus_addr,
                                                          false,
                                                          NULL,
                                                          &(stream_data->outgoing_id));
            qdr_link_set_context(stream_data->out_link, stream_data);
        }
        else if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE) {
            int32_t stream_id = frame->hd.stream_id;
            qdr_http2_stream_data_t *stream_data = (qdr_http2_stream_data_t *)nghttp2_session_get_stream_user_data(session_data->session, stream_id);
            stream_data->message = qd_message();
        }
    }

    return 0;
}

/**
 *  nghttp2_on_header_callback: Called when nghttp2 library emits
 *  single header name/value pair.
 */
static int on_header_callback(nghttp2_session *session,
                              const nghttp2_frame *frame,
                              const uint8_t *name,
                              size_t namelen,
                              const uint8_t *value,
                              size_t valuelen,
                              uint8_t flags,
                              void *user_data)
{
    int32_t stream_id = frame->hd.stream_id;
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(session_data->session, stream_id);

    switch (frame->hd.type) {
        case NGHTTP2_HEADERS: {
            if (!stream_data->app_properties) {
                stream_data->app_properties = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
                qd_compose_start_map(stream_data->app_properties);
            }
            qd_compose_insert_string_n(stream_data->app_properties, (const char *)name, namelen);
            qd_compose_insert_string_n(stream_data->app_properties, (const char *)value, valuelen);
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 HEADER [%s=%s]", conn->conn_id, stream_id, (char *)name, (char *)value);
        }
        break;
        default:
            break;
    }
    return 0;
}


static void link_deliver(qdr_http2_stream_data_t *stream_data, bool receive_complete)
{
    qd_composed_field_t  *header_prop = 0;
    qdr_http_connection_t *conn  = stream_data->session_data->conn;

    if (conn->ingress) {
        if (stream_data->reply_to && !stream_data->in_dlv) {
            header_prop = qd_message_compose_amqp(stream_data->message,
                                                  conn->config->address,
                                                  0,
                                                  stream_data->reply_to,
                                                  0,
                                                  0,
                                                  stream_data->stream_id);
            qd_compose_end_map(stream_data->app_properties);
            stream_data->app_properties = qd_compose(QD_PERFORMATIVE_BODY_DATA, stream_data->app_properties);
            if (receive_complete) {
                qd_compose_insert_binary(stream_data->app_properties, 0, 0);
            }
            qd_message_compose_3(stream_data->message, header_prop, stream_data->app_properties, receive_complete);
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"][L%"PRIu64"] Initiating qdr_link_deliver", conn->conn_id, stream_data->stream_id, stream_data->in_link->identity);
            stream_data->in_dlv = qdr_link_deliver(stream_data->in_link, stream_data->message, 0, false, 0, 0);
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"][L%"PRIu64"] Routed delivery dlv:%lx", conn->conn_id, stream_data->stream_id, stream_data->in_link->identity, (long) stream_data->in_dlv);

        }
    }
    else {
        header_prop = qd_message_compose_amqp(stream_data->message,
                                              stream_data->reply_to,
                                              0,
                                              0,
                                              0,
                                              0,
                                              stream_data->stream_id);
        qd_compose_end_map(stream_data->app_properties);
        stream_data->app_properties = qd_compose(QD_PERFORMATIVE_BODY_DATA, stream_data->app_properties);
        if (receive_complete) {
            qd_compose_insert_binary(stream_data->app_properties, 0, 0);
        }
        qd_message_compose_3(stream_data->message, header_prop, stream_data->app_properties, receive_complete);
        stream_data->in_dlv = qdr_link_deliver(stream_data->in_link, stream_data->message, 0, false, 0, 0);
    }
}

static void write_buffers(qdr_http_connection_t *conn)
{
    qdr_http2_session_data_t *session_data = conn->session_data;
    size_t buffs_to_write = pn_raw_connection_write_buffers_capacity(conn->pn_raw_conn);
    size_t qd_buff_size = DEQ_SIZE(session_data->buffs);
    size_t num_buffs = qd_buff_size > buffs_to_write ? buffs_to_write : qd_buff_size;
    pn_raw_buffer_t raw_buffers[num_buffs];
    qd_buffer_t *qd_buff = DEQ_HEAD(session_data->buffs);

    int i = 0;
    int total_bytes = 0;
    while (i < num_buffs && qd_buff != 0) {
        raw_buffers[i].bytes = (char *)qd_buffer_base(qd_buff);
        size_t buffer_size = qd_buffer_size(qd_buff);
        raw_buffers[i].capacity = buffer_size;
        raw_buffers[i].size = buffer_size;
        total_bytes += buffer_size;
        raw_buffers[i].offset = 0;
        raw_buffers[i].context = (uintptr_t) qd_buff;
        DEQ_REMOVE_HEAD(session_data->buffs);
        qd_buff = DEQ_HEAD(session_data->buffs);
        i ++;
    }
    qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i] Writing %i bytes in write_buffers", conn->conn_id, total_bytes);
    if (i >0) {
        size_t num_buffers_written = pn_raw_connection_write_buffers(session_data->conn->pn_raw_conn, raw_buffers, num_buffs);
        assert(num_buffs == num_buffers_written);
    }


}

//static void send_window_update_frame(qdr_http2_session_data_t *session_data, int32_t stream_id)
//{
//    int rv = nghttp2_submit_window_update(session_data->session, NGHTTP2_FLAG_NONE, stream_id, 65536);
//    if (rv != 0) {
//        printf ("Fatal error in nghttp2_submit_window_update\n");
//    }
//    nghttp2_session_send(session_data->session);
//    write_buffers(session_data);
//}


static void send_settings_frame(qdr_http_connection_t *conn)
{
    qdr_http2_session_data_t *session_data = conn->session_data;
    nghttp2_settings_entry iv[3] = {{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
                                    {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 65536},
                                    {NGHTTP2_SETTINGS_ENABLE_PUSH, 0}};

    // You must call nghttp2_session_send after calling nghttp2_submit_settings
    int rv = nghttp2_submit_settings(session_data->session, NGHTTP2_FLAG_NONE, iv, ARRLEN(iv));
    if (rv != 0) {
        qd_log(http_adaptor->log_source, QD_LOG_INFO, "[C%i] Fatal error sending settings frame, rv=%i", conn->conn_id, rv);
    }
    nghttp2_session_send(session_data->session);
    write_buffers(session_data->conn);
}


static int on_frame_recv_callback(nghttp2_session *session,
                                  const nghttp2_frame *frame, void *user_data)
{
    qdr_http_connection_t *conn = (qdr_http_connection_t *)user_data;
    qdr_http2_session_data_t *session_data = conn->session_data;

    int32_t stream_id = frame->hd.stream_id;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(session_data->session, stream_id);

    switch (frame->hd.type) {
    case NGHTTP2_SETTINGS: {
        qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 SETTINGS frame received", conn->conn_id, stream_id);
    }
    break;
    case NGHTTP2_WINDOW_UPDATE:
        qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 WINDOW_UPDATE frame received", conn->conn_id, stream_id);
    break;
    case NGHTTP2_DATA: {
        qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 DATA frame received", conn->conn_id, stream_id);
        if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 END_STREAM flag received", conn->conn_id, stream_id);
            MSG_CONTENT(stream_data->message)->receive_complete = true;
        }
        if (stream_data->in_dlv) {
            qdr_delivery_continue(http_adaptor->core, stream_data->in_dlv, false);
        }
    }
    break;
    case NGHTTP2_HEADERS:{
        qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 HEADERS frame received", conn->conn_id, stream_id);
        if (frame->hd.flags & NGHTTP2_FLAG_END_HEADERS) {
            /* All the headers have been received. Send out the AMQP message */
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%"PRId32"] HTTP2 END_HEADERS flag received", conn->conn_id, stream_id);
            stream_data->entire_header_arrived = true;
            // All header fields have been received
            // End the map.
            bool receive_complete = false;
            if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
                receive_complete = true;
            }

            MSG_CONTENT(stream_data->message)->receive_complete = receive_complete;
            link_deliver(stream_data, receive_complete);
        }
    }
    break;
    default:
        break;
  }
    return 0;
}

qdr_http_connection_t *qdr_http_connection_ingress(qd_http_lsnr_t* listener)
{
    qdr_http_connection_t* ingress_http_conn = NEW(qdr_http_connection_t);
    ingress_http_conn->ingress = true;
    ingress_http_conn->context.context = ingress_http_conn;
    ingress_http_conn->context.handler = &handle_connection_event;
    ingress_http_conn->config = &(listener->config);
    ingress_http_conn->server = listener->server;
    ingress_http_conn->pn_raw_conn = pn_raw_connection();

    ingress_http_conn->session_data = new_qdr_http2_session_data_t();
    ZERO(ingress_http_conn->session_data);
    DEQ_INIT(ingress_http_conn->session_data->streams);
    ingress_http_conn->session_data->conn = ingress_http_conn;

    nghttp2_session_server_new(&(ingress_http_conn->session_data->session), (nghttp2_session_callbacks*)http_adaptor->callbacks, ingress_http_conn);

    pn_raw_connection_set_context(ingress_http_conn->pn_raw_conn, ingress_http_conn);
    pn_listener_raw_accept(listener->pn_listener, ingress_http_conn->pn_raw_conn);
    ingress_http_conn->connection_established = true;
    send_settings_frame(ingress_http_conn);
    return ingress_http_conn;
}

static void grant_read_buffers(qdr_http_connection_t *conn)
{
    pn_raw_buffer_t raw_buffers[READ_BUFFERS];
    // Give proactor more read buffers for the pn_raw_conn
    if (!pn_raw_connection_is_read_closed(conn->pn_raw_conn)) {
        size_t desired = pn_raw_connection_read_buffers_capacity(conn->pn_raw_conn);
        while (desired) {
            size_t i;
            for (i = 0; i < desired && i < READ_BUFFERS; ++i) {
                qd_buffer_t *buf = qd_buffer();
                raw_buffers[i].bytes = (char*) qd_buffer_base(buf);
                raw_buffers[i].capacity = qd_buffer_capacity(buf);
                raw_buffers[i].size = 0;
                raw_buffers[i].offset = 0;
                raw_buffers[i].context = (uintptr_t) buf;
            }
            desired -= i;
            pn_raw_connection_give_read_buffers(conn->pn_raw_conn, raw_buffers, i);
        }
    }
}


void qd_http_listener_decref(qd_http_lsnr_t* li)
{
    if (li && sys_atomic_dec(&li->ref_count) == 1) {
        qd_http_free_bridge_config(&li->config);
        free_qd_http_lsnr_t(li);
    }
}

static void qdr_http_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
}


static void qdr_http_flow(void *context, qdr_link_t *link, int credit)
{
}


static void qdr_http_offer(void *context, qdr_link_t *link, int delivery_count)
{
}


static void qdr_http_drained(void *context, qdr_link_t *link)
{
}


static void qdr_http_drain(void *context, qdr_link_t *link, bool mode)
{
}

static int qdr_http_get_credit(void *context, qdr_link_t *link)
{
    return 10;
}


static void qdr_http_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
}


static void qdr_http_conn_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
}


static void qdr_http_conn_trace(void *context, qdr_connection_t *conn, bool trace)
{
}


static void qdr_http_first_attach(void *context, qdr_connection_t *conn, qdr_link_t *link,
                                 qdr_terminus_t *source, qdr_terminus_t *target,
                                 qd_session_class_t session_class)
{
}


static void qdr_copy_reply_to(qdr_http2_stream_data_t* stream_data, qd_iterator_t* reply_to)
{
    int length = qd_iterator_length(reply_to);
    stream_data->reply_to = malloc(length + 1);
    qd_iterator_strncpy(reply_to, stream_data->reply_to, length + 1);
}


static void qdr_http_second_attach(void *context, qdr_link_t *link,
                                  qdr_terminus_t *source, qdr_terminus_t *target)
{
    qdr_http2_stream_data_t *stream_data =  (qdr_http2_stream_data_t*)qdr_link_get_context(link);
    if (stream_data) {
        if (qdr_link_direction(link) == QD_OUTGOING && source->dynamic) {
            if (stream_data->session_data->conn->ingress) {
                qdr_copy_reply_to(stream_data, qdr_terminus_get_address(source));
                link_deliver(stream_data, MSG_CONTENT(stream_data->message)->receive_complete);
                grant_read_buffers(stream_data->session_data->conn);
            }
            qdr_link_flow(http_adaptor->core, link, 10, false);
        }
    }
}

static void qdr_http_activate(void *notused, qdr_connection_t *c)
{
    qdr_http_connection_t* conn = (qdr_http_connection_t*) qdr_connection_get_context(c);
    if (conn) {
        if (conn->pn_raw_conn) {
            qd_log(http_adaptor->log_source, QD_LOG_INFO, "[C%i] Activation triggered, calling pn_raw_connection_wake()", conn->conn_id);
            pn_raw_connection_wake(conn->pn_raw_conn);
        } else if (conn->activate_timer) {
            // On egress, the raw connection is only created once the
            // first part of the message encapsulating the
            // client->server half of the stream has been
            // received. Prior to that however a subscribing link (and
            // its associated connection must be setup), for which we
            // fake wakeup by using a timer.
            qd_log(http_adaptor->log_source, QD_LOG_INFO, "[C%i] Activation triggered, no socket yet so scheduling timer", conn->conn_id);
            qd_timer_schedule(conn->activate_timer, 0);
        } else {
            qd_log(http_adaptor->log_source, QD_LOG_ERROR, "[C%i] Cannot activate", conn->conn_id);
        }
    }
}

static int qdr_http_push(void *context, qdr_link_t *link, int limit)
{
    return qdr_link_process_deliveries(http_adaptor->core, link, limit);
}


static void http_connector_establish(qdr_http_connection_t *conn)
{
    qd_log(http_adaptor->log_source, QD_LOG_INFO, "[C%i] Connecting to: %s", conn->conn_id, conn->config->host_port);
    conn->pn_raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->pn_raw_conn, conn);
    pn_proactor_raw_connect(qd_server_proactor(conn->server), conn->pn_raw_conn, conn->config->host_port);
}

ssize_t read_callback(nghttp2_session *session,
                                  int32_t stream_id, uint8_t *buf,
                                  size_t length, uint32_t *data_flags,
                                  nghttp2_data_source *source,
                                  void *user_data)
{
    qd_buffer_t *qd_buff = source->ptr;
    buf = qd_buffer_base(qd_buff);
    size_t ret_val = qd_buffer_size(qd_buff);
    return ret_val;
}

uint64_t handle_outgoing_http(qdr_http2_stream_data_t *stream_data, qdr_link_t *link)
{
    qdr_http2_session_data_t *session_data = stream_data->session_data;
    qdr_http_connection_t *conn = session_data->conn;
    qd_message_t *message = stream_data->message;

    if (stream_data->out_dlv) {

        if (!stream_data->header_sent) {
            stream_data->header_sent = true;

            // The HTTP Path is in the AMQP to field.
            //qd_iterator_t *to = qd_message_field_iterator(message, QD_FIELD_TO);
            //char *path = (char *)qd_iterator_copy(to);

            //qd_iterator_t *subject = qd_message_field_iterator(message, QD_FIELD_SUBJECT);
            //char *http_method = (char *)qd_iterator_copy(subject);

            //qd_iterator_t *ct = qd_message_field_iterator(message, QD_FIELD_CONTENT_TYPE);
            //char *content_type = (char *)qd_iterator_copy(ct);

            qd_iterator_t *app_properties_iter = qd_message_field_iterator(message, QD_FIELD_APPLICATION_PROPERTIES);
            qd_parsed_field_t *app_properties_fld = qd_parse(app_properties_iter);

            uint32_t count = qd_parse_sub_count(app_properties_fld);

            nghttp2_nv hdrs[count];

            int stream_id = stream_data->session_data->conn->ingress?stream_data->stream_id: -1;

            for (uint32_t idx = 0; idx < count; idx++) {
                qd_parsed_field_t *key = qd_parse_sub_key(app_properties_fld, idx);
                qd_parsed_field_t *val = qd_parse_sub_value(app_properties_fld, idx);
                qd_iterator_t *key_raw = qd_parse_raw(key);
                qd_iterator_t *val_raw = qd_parse_raw(val);

                hdrs[idx].name = (uint8_t *)qd_iterator_copy(key_raw);
                hdrs[idx].value = (uint8_t *)qd_iterator_copy(val_raw);
                hdrs[idx].namelen = qd_iterator_length(key_raw);
                hdrs[idx].valuelen = qd_iterator_length(val_raw);
                hdrs[idx].flags = NGHTTP2_NV_FLAG_NONE;
                qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing HEADER [%s=%s]", conn->conn_id, stream_id, (char *)hdrs[idx].name, (char *)hdrs[idx].value);
            }

            /*
             * case NGHTTP2_HEADERS
             * case NGHTTP2_PRIORITY
             * case NGHTTP2_RST_STREAM
             * case NGHTTP2_SETTINGS
             * case NGHTTP2_PUSH_PROMISE
             * case NGHTTP2_PING
             * case NGHTTP2_CONTINUATION
             * case NGHTTP2_GOAWAY
             * case NGHTTP2_WINDOW_UPDATE
             */
            // This does not really submit the request. We need to read the bytes
            //nghttp2_session_set_next_stream_id(session_data->session, stream_data->stream_id);
            stream_data->stream_id = nghttp2_submit_headers(session_data->session,
                                                            0,
                                                            stream_id, NULL, hdrs,
                                                            count,
                                                            stream_data);

            nghttp2_session_send(session_data->session);
            write_buffers(session_data->conn);

        }

        qd_message_body_data_t *out_body_data = 0;
        qd_message_body_data_result_t body_data_result = qd_message_next_body_data(message, &out_body_data);
        bool has_out_body_data = false;

        char *body = 0;
        has_out_body_data = true;
        switch (body_data_result) {
        case QD_MESSAGE_BODY_DATA_OK: {
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing QD_MESSAGE_BODY_DATA_OK", conn->conn_id, stream_data->stream_id);
            qd_iterator_t *body_iter = qd_message_body_data_iterator(out_body_data);
            body = (char*) qd_iterator_copy(body_iter);
            if (body) {
                conn->data_prd.source.ptr = body;
                printf("handle_outgoing_http: message body-data received: %s\n", body);
                nghttp2_submit_data(session_data->session, 0, stream_data->stream_id, &conn->data_prd);
            }
            free(body);
            qd_iterator_free(body_iter);
            break;
        }

        case QD_MESSAGE_BODY_DATA_INCOMPLETE:
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing QD_MESSAGE_BODY_DATA_INCOMPLETE", conn->conn_id, stream_data->stream_id);
            break;

        case QD_MESSAGE_BODY_DATA_NO_MORE: {
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing QD_MESSAGE_BODY_DATA_NO_MORE", conn->conn_id, stream_data->stream_id);
            qd_message_set_send_complete(message);
            qdr_link_flow(http_adaptor->core, link, 1, false);
            nghttp2_submit_data(session_data->session, NGHTTP2_FLAG_END_STREAM, stream_data->stream_id, &conn->data_prd);
            return PN_ACCEPTED; // This will cause the delivery to be settled
        }

        case QD_MESSAGE_BODY_DATA_INVALID:
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing QD_MESSAGE_BODY_DATA_INVALID", conn->conn_id, stream_data->stream_id);
            qdr_link_flow(http_adaptor->core, link, 1, false);
            return PN_REJECTED;

        case QD_MESSAGE_BODY_DATA_NOT_DATA:
            qd_log(http_adaptor->log_source, QD_LOG_TRACE, "[C%i][S%i] HTTP2 Outgoing QD_MESSAGE_BODY_DATA_NOT_DATA", conn->conn_id, stream_data->stream_id);
            qdr_link_flow(http_adaptor->core, link, 1, false);
            return PN_REJECTED;

        } // switch

        if (has_out_body_data)
            nghttp2_session_send(session_data->session);

    }
    return 0;
}

static uint64_t qdr_http_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qdr_http2_stream_data_t *stream_data =  (qdr_http2_stream_data_t*)qdr_link_get_context(link);
    qdr_http_connection_t *conn = stream_data->session_data->conn;

    if (link == stream_data->session_data->conn->stream_dispatcher) {
        qd_message_t *msg = qdr_delivery_message(delivery);
        qd_iterator_t     *iter  = qd_message_field_iterator_typed(msg, QD_FIELD_CORRELATION_ID);
        qd_parsed_field_t *cid_field = qd_parse(iter);
        uint32_t stream_id = qd_parse_as_int(cid_field);

        qdr_http2_stream_data_t *stream_data = create_http2_stream_data(conn->session_data, stream_id);

        stream_data->message = qdr_delivery_message(delivery);
        stream_data->out_dlv = delivery;
        conn->initial_stream = stream_data;

        qdr_terminus_t *source = qdr_terminus(0);
        qdr_terminus_set_address(source, conn->config->address);
        stream_data->out_link = qdr_link_first_attach(conn->qdr_conn,
                                                     QD_OUTGOING,
                                                     source,           //qdr_terminus_t   *source,
                                                     qdr_terminus(0),  //qdr_terminus_t   *target,
                                                     "tcp.egress.out", //const char       *name,
                                                     0,                //const char       *terminus_addr,
                                                     true,
                                                     delivery,
                                                     &(stream_data->outgoing_id));
        conn->initial_stream->out_link = stream_data->out_link;
        qdr_link_set_context(stream_data->out_link, stream_data);
        qd_iterator_t *fld_iter = qd_message_field_iterator(msg, QD_FIELD_REPLY_TO);
        char *reply_to = (char *)qd_iterator_copy(fld_iter);
        stream_data->reply_to = malloc(qd_iterator_length(fld_iter) + 1);
        strcpy(stream_data->reply_to, reply_to);

        printf ("qdr_http_deliver if stream_dispatcher reply_to is %s\n", (char *)reply_to);

        // Sender link
        qdr_terminus_t *target = qdr_terminus(0);
        qdr_terminus_set_address(target, reply_to);
        stream_data->in_link = qdr_link_first_attach(conn->qdr_conn,
                                                     QD_INCOMING,
                                                     qdr_terminus(0),  //qdr_terminus_t   *source,
                                                     target, //qdr_terminus_t   *target,
                                                     "http.egress.in",  //const char       *name,
                                                     0,                //const char       *terminus_addr,
                                                     false,
                                                     0,
                                                     &(stream_data->incoming_id));

        // TODO - This is wrong
        qdr_link_set_context(stream_data->in_link, stream_data);
        printf ("qdr_http_deliver stream_data->session_data->conn->stream_dispatcher %p\n", (void *)stream_data->in_link);

        //Let's make an outbound connection to the configured connector.
        qdr_http_connection_t *conn = stream_data->session_data->conn;
        if (!conn->connection_established) {
            if (!conn->ingress) {
                http_connector_establish(conn);
            }
        }
    }
    else if (stream_data) {
        if (conn->connection_established) {
            if (conn->ingress) {
                stream_data->message = qdr_delivery_message(delivery);
                stream_data->out_dlv = delivery;
            }
            return handle_outgoing_http(stream_data, link);
        }
    }
    return 0;
}

void qd_http_connector_decref(qd_http_connector_t* c)
{
    if (c && sys_atomic_dec(&c->ref_count) == 1) {
        qd_http_free_bridge_config(&c->config);
        free_qd_http_connector_t(c);
    }
}


void qd_http2_delete_connector(qd_dispatch_t *qd, qd_http_connector_base_t *base)
{
    qd_http_connector_t *connector = (qd_http_connector_t*) base;
    if (connector) {
        //TODO: cleanup and close any associated active connections
        DEQ_REMOVE(http_adaptor->connectors, connector);
        qd_http_connector_decref(connector);
    }
}


static int handle_incoming_http(qdr_http_connection_t *conn)
{
    qd_buffer_list_t buffers;
    DEQ_INIT(buffers);
    pn_raw_buffer_t raw_buffers[READ_BUFFERS];
    size_t n;
    int count = 0;
    while ( (n = pn_raw_connection_take_read_buffers(conn->pn_raw_conn, raw_buffers, READ_BUFFERS)) ) {
        for (size_t i = 0; i < n && raw_buffers[i].bytes; ++i) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            uint32_t raw_buff_size = raw_buffers[i].size;
            qd_buffer_insert(buf, raw_buff_size);
            qd_log(http_adaptor->log_source, QD_LOG_DEBUG, "[C%i] Inserting qd_buffer of size %"PRIu32" ", conn->conn_id, raw_buff_size);
            count += raw_buffers[i].size;
            DEQ_INSERT_TAIL(buffers, buf);
        }
    }
    grant_read_buffers(conn);

    //
    // Read each buffer in the buffer chain and call nghttp2_session_mem_recv with each buffer content
    //
    qd_buffer_t *buf = DEQ_HEAD(buffers);
    qd_buffer_t *curr_buf = 0;
    while (buf) {
        nghttp2_session_mem_recv(conn->session_data->session, qd_buffer_base(buf), qd_buffer_size(buf));
        curr_buf = buf;
        DEQ_REMOVE_HEAD(buffers);
        buf = DEQ_HEAD(buffers);
        qd_buffer_free(curr_buf);
    }

    return count;
}


qdr_http_connection_t *qdr_http_connection_ingress_accept(qdr_http_connection_t* ingress_http_conn)
{
    ingress_http_conn->remote_address = get_address_string(ingress_http_conn->pn_raw_conn);
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_INCOMING, //qd_direction_t   dir,
                                                      ingress_http_conn->remote_address,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "HttpAdaptor",    //const char      *container,
                                                      pn_data(0),     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      // set if remote is a qdrouter
                                                      0);    //const qdr_router_version_t *version)

    qdr_connection_t *conn = qdr_connection_opened(http_adaptor->core,
                                                   http_adaptor->adaptor,
                                                   true,
                                                   QDR_ROLE_NORMAL,
                                                   1,
                                                   qd_server_allocate_connection_id(ingress_http_conn->server),
                                                   0,
                                                   0,
                                                   false,
                                                   false,
                                                   false,
                                                   false,
                                                   250,
                                                   0,
                                                   info,
                                                   0,
                                                   0);

    ingress_http_conn->qdr_conn = conn;
    ingress_http_conn->conn_id = conn->identity;
    qdr_connection_set_context(conn, ingress_http_conn);
    //grant_read_buffers(ingress_http_conn);
    return ingress_http_conn;
}


static void handle_connection_event(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http_connection_t *conn = (qdr_http_connection_t*) context;
    qd_log_source_t *log = http_adaptor->log_source;
    switch (pn_event_type(e)) {
    case PN_RAW_CONNECTION_CONNECTED: {
        if (conn->ingress) {
            qdr_http_connection_ingress_accept(conn);
            qd_log(log, QD_LOG_INFO, "[C%i] Accepted from %s", conn->conn_id, conn->remote_address);
        } else {
            qd_log(log, QD_LOG_INFO, "[C%i] Connected", conn->conn_id);
            conn->connection_established = true;
            handle_outgoing_http(conn->initial_stream, conn->initial_stream->out_link);
            qdr_connection_process(conn->qdr_conn);
        }
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ: {
        qd_log(log, QD_LOG_INFO, "[C%i] Closed for reading", conn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        qd_log(log, QD_LOG_INFO, "[C%i] Closed for writing", conn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qdr_connection_closed(conn->qdr_conn);
        //free_qdr_http_connection(conn);
        qd_log(log, QD_LOG_INFO, "[C%i] Disconnected", conn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(log, QD_LOG_INFO, "[C%i] Need write buffers", conn->conn_id);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        grant_read_buffers(conn);
        qd_log(log, QD_LOG_INFO, "[C%i] Need read buffers", conn->conn_id);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(log, QD_LOG_INFO, "[C%i] Wake-up", conn->conn_id);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        int read = handle_incoming_http(conn);
        qd_log(log, QD_LOG_INFO, "[C%i] Read %i bytes", conn->conn_id, read);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        pn_raw_buffer_t buffs[WRITE_BUFFERS];
        size_t n;
        size_t written = 0;
        while ( (n = pn_raw_connection_take_written_buffers(conn->pn_raw_conn, buffs, WRITE_BUFFERS)) ) {
            for (size_t i = 0; i < n; ++i) {
                qd_buffer_t *qd_buff = (qd_buffer_t *) buffs[i].context;
                assert(qd_buff);
                if (qd_buff)
                    qd_buffer_free(qd_buff);
            }
        }
        qd_log(log, QD_LOG_INFO, "[C%i] Wrote %i bytes", conn->conn_id, written);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    default:
        break;
    }
}


static void handle_listener_event(pn_event_t *e, qd_server_t *qd_server, void *context) {
    qd_log_source_t *log = http_adaptor->log_source;

    qd_http_lsnr_t *li = (qd_http_lsnr_t*) context;
    const char *host_port = li->config.host_port;

    switch (pn_event_type(e)) {
        case PN_LISTENER_OPEN: {
            qd_log(log, QD_LOG_NOTICE, "Listening on %s", host_port);
        }
        break;

        case PN_LISTENER_ACCEPT: {
            qd_log(log, QD_LOG_INFO, "Accepting HTTP connection on %s", host_port);
            qdr_http_connection_ingress(li);
        }
        break;

        case PN_LISTENER_CLOSE:
            break;

        default:
            break;
    }
}


static qd_http_lsnr_t *qd_http_lsnr(qd_server_t *server)
{
    qd_http_lsnr_t *li = new_qd_http_lsnr_t();
    if (!li)
        return 0;
    ZERO(li);
    sys_atomic_init(&li->ref_count, 1);
    li->server = server;
    li->context.context = li;
    li->context.handler = &handle_listener_event;
    return li;
}


static const int BACKLOG = 50;  /* Listening backlog */

static bool http_listener_listen(qd_http_lsnr_t *li) {
   li->pn_listener = pn_listener();
    if (li->pn_listener) {
        pn_listener_set_context(li->pn_listener, &li->context);
        pn_proactor_listen(qd_server_proactor(li->server), li->pn_listener, li->config.host_port, BACKLOG);
        sys_atomic_inc(&li->ref_count); /* In use by proactor, PN_LISTENER_CLOSE will dec */
        /* Listen is asynchronous, log "listening" message on PN_LISTENER_OPEN event */
    } else {
        qd_log(http_adaptor->log_source, QD_LOG_CRITICAL, "Failed to create listener for %s",
               li->config.host_port);
     }
    return li->pn_listener;
}


qd_http_listener_base_t *qd_http2_configure_listener(qd_dispatch_t *qd, const qd_http_bridge_config_t *config, qd_entity_t *entity)
{
    qd_http_lsnr_t *li = qd_http_lsnr(qd->server);
    if (!li) {
        qd_log(http_adaptor->log_source, QD_LOG_ERROR, "Unable to create http listener: no memory");
        return 0;
    }

    li->config = *config;
    //DEQ_ITEM_INIT(li);
    DEQ_INSERT_TAIL(http_adaptor->listeners, li);
    qd_log(http_adaptor->log_source, QD_LOG_INFO, "Configured HTTP_ADAPTOR listener on %s", (&li->config)->host_port);
    http_listener_listen(li);
    return (qd_http_listener_base_t*) li;
}


void qd_http2_delete_listener(qd_dispatch_t *qd, qd_http_listener_base_t *base)
{
    // TBD?
}


static qd_http_connector_t *qd_http_connector(qd_server_t *server)
{
    qd_http_connector_t *c = new_qd_http_connector_t();
    if (!c) return 0;
    ZERO(c);
    sys_atomic_init(&c->ref_count, 1);
    c->server      = server;
    return c;
}

static void on_activate(void *context)
{
    qdr_http_connection_t* conn = (qdr_http_connection_t*) context;

    qd_log(http_adaptor->log_source, QD_LOG_INFO, "[C%i] on_activate", conn->conn_id);
    while (qdr_connection_process(conn->qdr_conn)) {}
}



qdr_http_connection_t *qdr_http_connection_egress(qd_http_connector_t *connector)
{
    qdr_http_connection_t* egress_conn = NEW(qdr_http_connection_t);
    ZERO(egress_conn);
    //FIXME: this is only needed while waiting for raw_connection_wake
    //functionality in proton
    egress_conn->activate_timer = qd_timer(http_adaptor->core->qd, on_activate, egress_conn);

    egress_conn->ingress = false;
    egress_conn->context.context = egress_conn;
    egress_conn->context.handler = &handle_connection_event;
    egress_conn->config = &(connector->config);
    egress_conn->server = connector->server;
    egress_conn->data_prd.read_callback = read_callback;

    egress_conn->session_data = new_qdr_http2_session_data_t();
    ZERO(egress_conn->session_data);
    DEQ_INIT(egress_conn->session_data->streams);
    egress_conn->session_data->conn = egress_conn;

    nghttp2_session_client_new(&egress_conn->session_data->session, (nghttp2_session_callbacks*)http_adaptor->callbacks, (void *)egress_conn);

    //pn_raw_connection_set_context(egress_conn->pn_raw_conn, egress_conn);
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_OUTGOING, //qd_direction_t   dir,
                                                      egress_conn->config->host_port,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "httpAdaptor",    //const char      *container,
                                                      pn_data(0),     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      // set if remote is a qdrouter
                                                      0);    //const qdr_router_version_t *version)

    qdr_connection_t *conn = qdr_connection_opened(http_adaptor->core,
                                                   http_adaptor->adaptor,
                                                   true,
                                                   QDR_ROLE_NORMAL,
                                                   1,
                                                   qd_server_allocate_connection_id(egress_conn->server),
                                                   0,
                                                   0,
                                                   false,
                                                   false,
                                                   false,
                                                   false,
                                                   250,
                                                   0,
                                                   info,
                                                   0,
                                                   0);
    egress_conn->qdr_conn = conn;
    egress_conn->conn_id = conn->identity;
    qdr_connection_set_context(conn, egress_conn);

    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, egress_conn->config->address);
    egress_conn->stream_dispatcher = qdr_link_first_attach(conn,
                                                           QD_OUTGOING,
                                                           source,           //qdr_terminus_t   *source,
                                                           qdr_terminus(0),  //qdr_terminus_t   *target,
                                                           "stream_dispatcher", //const char       *name,
                                                           0,                //const char       *terminus_addr,
                                                           false,
                                                           0,
                                                           &(egress_conn->stream_dispatcher_id));
    // Create a dummy stream_data object and set that as context
    qdr_http2_stream_data_t *stream_data = new_qdr_http2_stream_data_t();
    ZERO(stream_data);

    stream_data->session_data = new_qdr_http2_session_data_t();
    ZERO(stream_data->session_data);
    stream_data->stream_id = 0;
    stream_data->session_data->conn = egress_conn;

    qdr_link_set_context(egress_conn->stream_dispatcher, stream_data);
    return egress_conn;
}


qd_http_connector_base_t *qd_http2_configure_connector(qd_dispatch_t *qd, const qd_http_bridge_config_t *config, qd_entity_t *entity)
{
    qd_http_connector_t *c = qd_http_connector(qd->server);
    if (!c) {
        qd_log(http_adaptor->log_source, QD_LOG_ERROR, "Unable to create http connector: no memory");
        return 0;
    }
    c->config = *config;
    DEQ_ITEM_INIT(c);
    DEQ_INSERT_TAIL(http_adaptor->connectors, c);
    qdr_http_connection_egress(c);
    return (void*) c;
}

static void qdr_http_adaptor_final(void *adaptor_context)
{
    qdr_http_adaptor_t *adaptor = (qdr_http_adaptor_t*) adaptor_context;
    qdr_protocol_adaptor_free(adaptor->core, adaptor->adaptor);
    free(adaptor);
    http_adaptor =  NULL;
}

/**
 * This initialization function will be invoked when the router core is ready for the protocol
 * adaptor to be created.  This function must:
 *
 *   1) Register the protocol adaptor with the router-core.
 *   2) Prepare the protocol adaptor to be configured.
 */
static void qdr_http_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    qdr_http_adaptor_t *adaptor = NEW(qdr_http_adaptor_t);
    adaptor->core    = core;
    adaptor->adaptor = qdr_protocol_adaptor(core,
                                            "http",                // name
                                            adaptor,              // context
                                            qdr_http_activate,                    // activate
                                            qdr_http_first_attach,
                                            qdr_http_second_attach,
                                            qdr_http_detach,
                                            qdr_http_flow,
                                            qdr_http_offer,
                                            qdr_http_drained,
                                            qdr_http_drain,
                                            qdr_http_push,
                                            qdr_http_deliver,
                                            qdr_http_get_credit,
                                            qdr_http_delivery_update,
                                            qdr_http_conn_close,
                                            qdr_http_conn_trace);
    adaptor->log_source = qd_log_source(QD_HTTP_LOG_SOURCE);
    adaptor->protocol_log_source = qd_log_source("PROTOCOL");
    *adaptor_context = adaptor;
    DEQ_INIT(adaptor->listeners);
    DEQ_INIT(adaptor->connectors);

    nghttp2_session_callbacks *callbacks;
    nghttp2_session_callbacks_new(&callbacks);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame_recv_callback);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, on_begin_headers_callback);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header_callback);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close_callback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk_recv_callback);
    nghttp2_session_callbacks_set_send_callback(callbacks, send_callback);

    adaptor->callbacks = callbacks;
    http_adaptor = adaptor;
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("http-adaptor", qdr_http_adaptor_init, qdr_http_adaptor_final)
