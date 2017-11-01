#include "ae/ae.h"
#include <curl/multi.h>
#include <Python.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdbool.h>
#include "structmember.h"

#define NO_ACTIVE_TIMER_ID -1
#define SESSION_AE_LOOP(session) (((Session*)session)->loop->event_loop)

#define DEBUG 0

#if defined(DEBUG) && DEBUG > 0
#include <sys/syscall.h>
 #define DEBUG_PRINT(fmt, args...) fprintf(stderr, "DEBUG: %s:%d:%s() tid=%ld: " fmt "\n", \
    __FILE__, __LINE__, __func__, (long)syscall(SYS_gettid), ##args)
#else
 #define DEBUG_PRINT(fmt, args...) /* Don't do anything in release builds */
#endif


typedef struct {
    PyObject_HEAD
    aeEventLoop *event_loop;
    PyThreadState* thread_state;
    bool stop;
    int req_in_read;
    int req_in_write;
    int req_out_read;
    int req_out_write;
    int stop_read;
    int stop_write;
    int curl_easy_cleanup_read;
    int curl_easy_cleanup_write;
} EventLoop;


typedef struct {
    PyObject_HEAD
    EventLoop *loop;
    CURLM *multi;
    CURLSH *shared;
    long long timer_id;
} Session;


struct BufferNode {
    int len;
    char *buffer;
    struct BufferNode *next;
};


typedef struct {
    char* method;
    char* url;
    char* auth;
    PyObject* cookies;
    int cookies_len;
    char** cookies_str;
    PyObject* future;
    struct curl_slist* headers;
    int req_data_len;
    char* req_data_buf;
    Session* session;
    CURL *curl;
    CURLcode result;
    struct BufferNode *header_buffer_head;
    struct BufferNode *header_buffer_tail;
    struct BufferNode *body_buffer_head;
    struct BufferNode *body_buffer_tail;
    int dummy;
} AcRequestData;


typedef struct {
    PyObject_HEAD
    struct BufferNode *header_buffer;
    struct BufferNode *body_buffer;
    Session *session;
    CURL *curl;
} Response;


void free_buffer_nodes(struct BufferNode *start) {
    struct BufferNode *node = start;
    while(node != NULL)
    {
        struct BufferNode *next = node->next;
        free(node->buffer);
        free(node);
        node = next;
    };
}


static void Response_dealloc(Response *self)
{
    DEBUG_PRINT("response=%p", self);
    free_buffer_nodes(self->header_buffer);
    free_buffer_nodes(self->body_buffer);
    write(self->session->loop->curl_easy_cleanup_write, &self->curl, sizeof(CURL *));
    Py_XDECREF(self->session);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


PyObject * get_buffer_as_pylist(struct BufferNode *start) 
{
    int i = 0, len = 0;
    PyObject* list;
    struct BufferNode *node = start;
    while(node != NULL)
    {
        len++;
        node = node->next;
    };
    list = PyList_New(len);
    node = start;;
    while(node != NULL)
    {
        PyList_SET_ITEM(list, i++, PyBytes_FromStringAndSize(node->buffer, node->len));
        node = node->next;
    };
    DEBUG_PRINT("list=%p", list);
    return list;
}


static PyObject *
Response_get_header(Response *self, PyObject *args)
{
    DEBUG_PRINT("");
    return get_buffer_as_pylist(self->header_buffer);
}


static PyObject *
Response_get_body(Response *self, PyObject *args)
{
    DEBUG_PRINT("");
    return get_buffer_as_pylist(self->body_buffer);
}


PyObject *resp_get_info_long(Response *self, CURLINFO info)
{
    long value;
    curl_easy_getinfo(self->curl, info, &value);
    return PyLong_FromLong(value);
}

PyObject *resp_get_info_double(Response *self, CURLINFO info)
{
    double value;
    curl_easy_getinfo(self->curl, info, &value);
    return PyFloat_FromDouble(value);
}

PyObject *resp_get_info_unicode(Response *self, CURLINFO info)
{
    char *value = NULL;
    curl_easy_getinfo(self->curl, info, &value);
    if(value != NULL) {
        return PyUnicode_FromString(value);
    }
    else {
        Py_RETURN_NONE;
    }
}

static PyObject *Response_get_effective_url(Response *self, PyObject *args)
{
    return resp_get_info_unicode(self, CURLINFO_EFFECTIVE_URL);
}

static PyObject *Response_get_response_code(Response *self, PyObject *args)
{
    return resp_get_info_long(self, CURLINFO_RESPONSE_CODE);
}

static PyObject *Response_get_total_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_TOTAL_TIME);
}

static PyObject *Response_get_namelookup_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_NAMELOOKUP_TIME);
}

static PyObject *Response_get_connect_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_CONNECT_TIME);
}

static PyObject *Response_get_appconnect_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_APPCONNECT_TIME);
}

static PyObject *Response_get_pretransfer_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_PRETRANSFER_TIME);
}

static PyObject *Response_get_starttransfer_time(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_STARTTRANSFER_TIME);
}

static PyObject *Response_get_size_upload(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_SIZE_UPLOAD);
}

static PyObject *Response_get_size_download(Response *self, PyObject *args)
{
    return resp_get_info_double(self, CURLINFO_SIZE_DOWNLOAD);
}

static PyObject *Response_get_primary_ip(Response *self, PyObject *args)
{
    return resp_get_info_unicode(self, CURLINFO_PRIMARY_IP);
}

static PyObject *Response_get_cookielist(Response *self, PyObject *args)
{
    struct curl_slist *start = NULL;
    struct curl_slist *node = NULL;
    int len = 0; int i = 0;
    PyObject *list = NULL;
    curl_easy_getinfo(self->curl, CURLINFO_COOKIELIST, &start);
    node = start;
    while(node != NULL) {
        len++;
        node = node->next;
    }
    list = PyList_New(len);
    node = start;
    while(node != NULL)
    {
        PyList_SET_ITEM(list, i++, PyUnicode_FromString(node->data));
        node = node->next;
    };
    curl_slist_free_all(start);
    return list;
}

static PyObject *Response_get_redirect_url(Response *self, PyObject *args)
{
    return resp_get_info_unicode(self, CURLINFO_REDIRECT_URL);
}



static PyMethodDef Response_methods[] = {
    {"get_effective_url", (PyCFunction)Response_get_effective_url, METH_NOARGS, ""},
    {"get_response_code", (PyCFunction)Response_get_response_code, METH_NOARGS, ""},
    {"get_total_time", (PyCFunction)Response_get_total_time, METH_NOARGS, ""},
    {"get_namelookup_time", (PyCFunction)Response_get_namelookup_time, METH_NOARGS, ""},
    {"get_connect_time", (PyCFunction)Response_get_connect_time, METH_NOARGS, ""},
    {"get_appconnect_time", (PyCFunction)Response_get_appconnect_time, METH_NOARGS, ""},
    {"get_pretransfer_time", (PyCFunction)Response_get_pretransfer_time, METH_NOARGS, ""},
    {"get_starttransfer_time", (PyCFunction)Response_get_starttransfer_time, METH_NOARGS, ""},
    {"get_size_upload", (PyCFunction)Response_get_size_upload, METH_NOARGS, ""},
    {"get_size_download", (PyCFunction)Response_get_size_download, METH_NOARGS, ""},
    {"get_primary_ip", (PyCFunction)Response_get_primary_ip, METH_NOARGS, ""},
    {"get_cookielist", (PyCFunction)Response_get_cookielist, METH_NOARGS, ""},
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, "Get the redirect URL or None"},
    {"get_header", (PyCFunction)Response_get_header, METH_NOARGS, "Get the header"},
    {"get_body", (PyCFunction)Response_get_body, METH_NOARGS, "Get the body"},
    {NULL, NULL, 0, NULL}
};


static PyMemberDef Response_members[] = {
    {NULL}
};


static PyTypeObject ResponseType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_acurl.Response",           /* tp_name */
    sizeof(Response),           /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Response_dealloc,           /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Response Type",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Response_methods,          /* tp_methods */
    Response_members,          /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};


void response_complete(Session *session) 
{
    DEBUG_PRINT("session=%p", session);
    int remaining_in_queue = 1;
    AcRequestData *rd;
    CURLMsg *msg;
    while(remaining_in_queue > 0) 
    {
        DEBUG_PRINT("calling curl_multi_info_read");
        msg = curl_multi_info_read(session->multi, &remaining_in_queue);
        if(msg == NULL) {
            break;
        }
        curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (void **)&rd);
        curl_multi_remove_handle(rd->session->multi, rd->curl);
        rd->result = msg->data.result;
        curl_slist_free_all(rd->headers);
        rd->headers = NULL;
        free(rd->req_data_buf);
        rd->req_data_buf = NULL;
        rd->req_data_len = 0;

        DEBUG_PRINT("writing to req_out_write");
        write(session->loop->req_out_write, &rd, sizeof(AcRequestData *));
    }
}


void socket_action_and_response_complete(Session *session, curl_socket_t socket, int ev_bitmask) 
{
    DEBUG_PRINT("session=%p socket=%d ev_bitmask=%d", session, socket, ev_bitmask);
    int running_handles;
    curl_multi_socket_action(session->multi, socket, ev_bitmask, &running_handles);
    DEBUG_PRINT("session_handles_after=%d", running_handles);
    response_complete(session);
}


size_t header_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    if(size * nmemb == 0) {
        return 0;
    }
    AcRequestData *rd = (AcRequestData *)userdata;
    struct BufferNode *node = (struct BufferNode *)malloc(sizeof(struct BufferNode));
    node->len = size * nmemb;
    node->buffer = (char*)malloc(node->len);
    memcpy(node->buffer, ptr, node->len);
    node->next = NULL;
    if(rd->header_buffer_head == NULL) {
        rd->header_buffer_head = node;
    }
    if(rd->header_buffer_tail != NULL) {
        rd->header_buffer_tail->next = node;
    }
    rd->header_buffer_tail = node;
    return node->len;
}
 

size_t body_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    if(size * nmemb == 0) {
        return 0;
    }
    AcRequestData *rd = (AcRequestData *)userdata;
    struct BufferNode *node = (struct BufferNode *)malloc(sizeof(struct BufferNode));
    node->len = size * nmemb;
    node->buffer = (char*)malloc(node->len);
    memcpy(node->buffer, ptr, node->len);
    node->next = NULL;
    if(rd->body_buffer_head == NULL) {
        rd->body_buffer_head = node;
    }
    if(rd->body_buffer_tail != NULL) {
        rd->body_buffer_tail->next = node;
    }
    rd->body_buffer_tail = node;
    return node->len;
}
 

void start_request(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    AcRequestData *rd;
    EventLoop *loop = (EventLoop*)clientData;
    read(loop->req_in_read, &rd, sizeof(AcRequestData *));
    DEBUG_PRINT("read AcRequestData");
    rd->curl = curl_easy_init();
    curl_easy_setopt(rd->curl, CURLOPT_SHARE, rd->session->shared);
    curl_easy_setopt(rd->curl, CURLOPT_URL, rd->url);
    curl_easy_setopt(rd->curl, CURLOPT_CUSTOMREQUEST, rd->method);
    //curl_easy_setopt(rd->curl, CURLOPT_VERBOSE, 1L); //DEBUG
    if(rd->headers != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_HTTPHEADER, rd->headers);
    }
    if(rd->auth != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_USERPWD, rd->auth);
    }
    for(int i=0; i < rd->cookies_len; i++) {
        DEBUG_PRINT("set cookie [%s]", rd->cookies_str[i]);
        curl_easy_setopt(rd->curl, CURLOPT_COOKIELIST, rd->cookies_str[i]);
    }
    if(rd->req_data_buf != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDSIZE, rd->req_data_len);
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDS, (char*)rd->req_data_buf);
    }
    curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(rd->curl, CURLOPT_PRIVATE, rd);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEFUNCTION, body_callback);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEDATA, rd);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERDATA, rd);
    free(rd->method);
    rd->method = NULL;
    free(rd->url);
    rd->url = NULL;
    if(rd->auth != NULL) {
        free(rd->auth);
        rd->auth = NULL;
    }
    free(rd->cookies_str);
    if(rd->dummy) {
        rd->result = CURLE_OK;
        curl_slist_free_all(rd->headers);
        free(rd->req_data_buf);
        write(loop->req_out_write, &rd, sizeof(AcRequestData *));
    }
    else {
        curl_multi_add_handle(rd->session->multi, rd->curl);
        DEBUG_PRINT("added handle");
        socket_action_and_response_complete(rd->session, CURL_SOCKET_TIMEOUT, 0);
    }
    return;
}


void stop_eventloop(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    char buffer[1];
    EventLoop *loop = (EventLoop*)clientData;
    read(loop->stop_read, buffer, sizeof(buffer));
    loop->stop = true;
}


void curl_easy_cleanup_in_eventloop(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    CURL *curl;
    read(fd, &curl, sizeof(CURL *));
    DEBUG_PRINT("curl=%p", curl);
    curl_easy_cleanup(curl);
}


static PyObject *
EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    EventLoop *self = (EventLoop *)type->tp_alloc(type, 0);
    int req_in[2];
    int req_out[2];
    int stop[2];
    int curl_easy_cleanup[2];
    if (self != NULL) {
        self->event_loop = aeCreateEventLoop(10000);
        pipe(req_in);
        self->req_in_read = req_in[0];
        self->req_in_write = req_in[1];
        pipe(req_out);
        self->req_out_read = req_out[0];
        self->req_out_write = req_out[1];
        pipe(stop);
        self->stop_read = stop[0];
        self->stop_write = stop[1];
        pipe(curl_easy_cleanup);
        self->curl_easy_cleanup_read = curl_easy_cleanup[0];
        self->curl_easy_cleanup_write = curl_easy_cleanup[1];
        if(aeCreateFileEvent(self->event_loop, self->req_in_read, AE_READABLE, start_request, self) == AE_ERR) {
            exit(1);
        }
        if(aeCreateFileEvent(self->event_loop, self->stop_read, AE_READABLE, stop_eventloop, self) == AE_ERR) {
            exit(1);
        }
        if(aeCreateFileEvent(self->event_loop, self->curl_easy_cleanup_read, AE_READABLE, curl_easy_cleanup_in_eventloop, NULL) == AE_ERR) {
            exit(1);
        }
    }
    return (PyObject *)self;
}


static void
EventLoop_dealloc(EventLoop *self)
{
    DEBUG_PRINT("response=%p", self);
    aeDeleteEventLoop(self->event_loop);
    close(self->req_in_read);
    close(self->req_in_write);
    close(self->req_out_read);
    close(self->req_out_write);
    close(self->stop_read);
    close(self->stop_write);
    close(self->curl_easy_cleanup_read);
    close(self->curl_easy_cleanup_write);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
EventLoop_main(EventLoop *self, PyObject *args)
{
    DEBUG_PRINT("Started");
    self->thread_state = PyEval_SaveThread();
    do {
        DEBUG_PRINT("Start of aeProcessEvents");
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS);
        DEBUG_PRINT("End of aeProcessEvents");
    } while(!self->stop);
    PyEval_RestoreThread(self->thread_state);
    DEBUG_PRINT("Ended");
    Py_RETURN_NONE;
}


static PyObject *
EventLoop_stop(PyObject *self, PyObject *args)
{
    write(((EventLoop*)self)->stop_write, '\0', 1);
    Py_RETURN_NONE;
}

static PyObject *
Eventloop_get_out_fd(PyObject *self, PyObject *args)
{
    DEBUG_PRINT("");
    return Py_BuildValue("i", ((EventLoop*)self)->req_out_read);
}


static PyObject *
Eventloop_get_completed(PyObject *self, PyObject *args)
{
    AcRequestData *rd;
    PyObject *rtn;
    read(((EventLoop*)self)->req_out_read, &rd, sizeof(AcRequestData *));
    DEBUG_PRINT("read AcRequestData; address=%p", rd);
    if(rd->result == CURLE_OK) {
        Response *response = PyObject_New(Response, (PyTypeObject *)&ResponseType);
        response->header_buffer = rd->header_buffer_head;
        response->body_buffer = rd->body_buffer_head;
        response->curl = rd->curl;
        response->session = rd->session;
        rtn = Py_BuildValue("ONN", Py_None, response, rd->future);
    }
    else {
        PyObject* error = PyUnicode_FromString(curl_easy_strerror(rd->result));
        free_buffer_nodes(rd->header_buffer_head);
        free_buffer_nodes(rd->body_buffer_head);
        curl_easy_cleanup(rd->curl);
        Py_XDECREF(rd->session);
        rtn = Py_BuildValue("NON", error, Py_None, rd->future);
    }
    if(rd->req_data_buf != NULL) {
        free(rd->req_data_buf);
    }
    Py_XDECREF(rd->cookies);
    free(rd);
    return rtn;
}


static PyMethodDef EventLoop_methods[] = {
    {"main", (PyCFunction)EventLoop_main, METH_NOARGS, "Run the event loop"},
    {"stop", EventLoop_stop, METH_NOARGS, "Stop the event loop"},
    {"get_out_fd", Eventloop_get_out_fd, METH_NOARGS, "Get the outbound file dscriptor"},
    {"get_completed", Eventloop_get_completed, METH_NOARGS, "Get the user_object, response and error"},
    {NULL, NULL, 0, NULL}
};


static PyMemberDef EventLoop_members[] = {
    {NULL}
};


static PyTypeObject EventLoopType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "acurl.EventLoop",           /* tp_name */
    sizeof(EventLoop),           /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)EventLoop_dealloc,           /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Event Loop Type",         /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    EventLoop_methods,         /* tp_methods */
    EventLoop_members,         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    EventLoop_new,             /* tp_new */
};


void socket_event(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    DEBUG_PRINT("eventloop=%p fd=%d clientData=%p mask=%d (readable=%d writable=%d)", eventLoop, fd, clientData, mask, mask & AE_READABLE, mask & AE_WRITABLE);
    int ev_bitmask = 0;
    if(mask & AE_READABLE) 
    {
        ev_bitmask |= CURL_CSELECT_IN;
    }
    if(mask & AE_WRITABLE)
    {
         ev_bitmask |= CURL_CSELECT_OUT;
    } 
    socket_action_and_response_complete((Session*)clientData, (curl_socket_t)fd, ev_bitmask);
}


int socket_callback(CURL *easy, curl_socket_t s, int what, void *userp, void *socketp)
{
    int result = 10; //FIXME fake value because of CURL_POLL_REMOVE case
    switch(what) {
        case CURL_POLL_NONE:
            DEBUG_PRINT("NONE socket=%d what=%d easy=%p", s, what, easy);
            //do nothing
            break;
        case CURL_POLL_IN:
            DEBUG_PRINT("IN socket=%d what=%d easy=%p", s, what, easy);
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE, socket_event, userp);
            aeDeleteFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_WRITABLE);
            break;
        case CURL_POLL_OUT:
            DEBUG_PRINT("OUT socket=%d what=%d easy=%p", s, what, easy);
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_WRITABLE, socket_event, userp);
            aeDeleteFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE);
            break;
        case CURL_POLL_INOUT:
            DEBUG_PRINT("INOUT socket=%d what=%d easy=%p", s, what, easy);
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE | AE_WRITABLE, socket_event, userp);
            break;
        case CURL_POLL_REMOVE:
            DEBUG_PRINT("REMOVE socket=%d what=%d easy=%p", s, what, easy);
            aeDeleteFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE | AE_WRITABLE);
            break;
    };
    if(result == AE_ERR) {
        fprintf(stderr, "socket_callback failed\n");
        exit(1);
    }
    return 0; 
}




int timeout(struct aeEventLoop *eventLoop, long long id, void *clientData) 
{
    DEBUG_PRINT("");
    Session *session = (Session*)clientData;
    session->timer_id = NO_ACTIVE_TIMER_ID;
    socket_action_and_response_complete(session, CURL_SOCKET_TIMEOUT, 0);
    return AE_NOMORE;
}


int timer_callback(CURLM *multi, long timeout_ms, void *userp)
{
    DEBUG_PRINT("timeout_ms=%ld", timeout_ms);
    Session *session = (Session*)userp;
    if(session->timer_id != NO_ACTIVE_TIMER_ID) {
        DEBUG_PRINT("DELETE timer_id=%ld", session->timer_id);
        aeDeleteTimeEvent(SESSION_AE_LOOP(session), session->timer_id);
        session->timer_id = NO_ACTIVE_TIMER_ID;
    }
    if(timeout_ms > 0) {
        if((session->timer_id = aeCreateTimeEvent(SESSION_AE_LOOP(session), timeout_ms, timeout, userp, NULL)) == AE_ERR) {
            fprintf(stderr, "timer_callback failed\n");
            exit(1);
        }
        DEBUG_PRINT("CREATE timer_id=%ld", session->timer_id);
    }
    else if(timeout_ms == 0) {
        socket_action_and_response_complete((Session*)userp, CURL_SOCKET_TIMEOUT, 0);
    }
    return 0;
}


static PyObject *
Session_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    Session *self;
    EventLoop *loop;
    
    static char *kwlist[] = {"loop", NULL};
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &loop)) {
        return NULL;
    }

    self = (Session *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    
    Py_INCREF(loop);
    self->loop = loop;
    self->timer_id = NO_ACTIVE_TIMER_ID;
    self->multi = curl_multi_init();
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETFUNCTION, socket_callback);
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETDATA, self);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERFUNCTION, timer_callback);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERDATA, self);
    curl_multi_setopt(self->multi, CURLOPT_MAXCONNECTS, 5);
    self->shared = curl_share_init();
    curl_share_setopt(self->shared, CURLSHOPT_SHARE, CURL_LOCK_DATA_COOKIE);
    curl_share_setopt(self->shared, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(self->shared, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
    return (PyObject *)self;
}


static void
Session_dealloc(Session *self)
{
    DEBUG_PRINT("response=%p", self);
    curl_multi_cleanup(self->multi);
    curl_share_cleanup(self->shared);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Session_request(Session *self, PyObject *args, PyObject *kwds)
{
    PyObject *future;
    char *method;
    char *url;
    PyObject *headers;
    char *auth;
    PyObject *cookies;
    int req_data_len = 0;
    char *req_data_buf = NULL;
    int dummy;
    
    static char *kwlist[] = {"future", "method", "url", "headers", "auth", "cookies", "data", "dummy", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OssO!zO!z#p", kwlist, &future, &method, &url, &PyTuple_Type, &headers, &auth, &PyTuple_Type, &cookies, &req_data_buf, &req_data_len, &dummy)) {
        return NULL;
    }
    AcRequestData *rd = (AcRequestData *)malloc(sizeof(AcRequestData));
    memset(rd, 0, sizeof(AcRequestData));
    Py_INCREF(self);
    rd->session = self;
    Py_INCREF(future);
    rd->future = future;

    rd->method = strdup(method);
    rd->url = strdup(url);
    if(req_data_buf != NULL) {
        req_data_buf = strdup(req_data_buf);
    }

    rd->headers = NULL;
    for(int i=0; i < PyTuple_GET_SIZE(headers); i++) {
        //curl_slist_append copies the string
        rd->headers = curl_slist_append(rd->headers, PyUnicode_AsUTF8(PyTuple_GET_ITEM(headers, i)));
    }
    
    Py_INCREF(cookies);
    rd->cookies = cookies;
    rd->cookies_len = PyTuple_GET_SIZE(cookies);
    if(rd->cookies_len > 0) {
        rd->cookies_str = (char**)calloc(PyTuple_GET_SIZE(cookies), sizeof(char*));
        for(int i=0; i < PyTuple_GET_SIZE(cookies); i++) {
            rd->cookies_str[i] = PyUnicode_AsUTF8(PyTuple_GET_ITEM(cookies, i));
        }
    } else {
        rd->cookies_str = NULL;
    }
    
    rd->auth = auth != NULL ? strdup(auth) : NULL;

    rd->req_data_len = req_data_len;
    rd->req_data_buf = req_data_buf;
    rd->dummy = dummy;

    write(self->loop->req_in_write, &rd, sizeof(AcRequestData *));
    DEBUG_PRINT("scheduling request");
    Py_RETURN_NONE;
}


static PyMethodDef Session_methods[] = {
    {"request", (PyCFunction)Session_request, METH_VARARGS | METH_KEYWORDS, "Send a request"},
    {NULL, NULL, 0, NULL}
};


static PyTypeObject SessionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "acurl.Session",           /* tp_name */
    sizeof(Session),           /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Session_dealloc,           /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Session Type",            /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Session_methods,           /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    Session_new,               /* tp_new */
};


const static char MODULE_NAME[] = "_acurl";


static PyMethodDef module_methods[] = {
    {NULL, NULL, 0, NULL}
};


static struct PyModuleDef _acurl_module = {
   PyModuleDef_HEAD_INIT,
   MODULE_NAME,
   NULL,
   -1,
   module_methods
};


PyMODINIT_FUNC
PyInit__acurl(void)
{
    PyObject* m;
    
    if (PyType_Ready(&SessionType) < 0)
        return NULL;

    if (PyType_Ready(&EventLoopType) < 0)
        return NULL;

    if (PyType_Ready(&ResponseType) < 0)
        return NULL;

    m = PyModule_Create(&_acurl_module);

    if(m != NULL) {
        curl_global_init(CURL_GLOBAL_ALL); // init curl library
        Py_INCREF(&SessionType);
        PyModule_AddObject(m, "Session", (PyObject *)&SessionType);
        Py_INCREF(&EventLoopType);
        PyModule_AddObject(m, "EventLoop", (PyObject *)&EventLoopType);
        Py_INCREF(&ResponseType);
        PyModule_AddObject(m, "Response", (PyObject *)&ResponseType);
    }
    
    return m;
}
