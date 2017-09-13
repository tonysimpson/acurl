#include "ae/ae.h"
#include <curl/multi.h>
#include <Python.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdbool.h>
#include "structmember.h"

#define NO_ACTIVE_TIMER_ID -1
#define MAX_COMPLETE_AT_ONCE 500
#define MAX_START_AT_ONCE MAX_COMPLETE_AT_ONCE
#define SESSION_AE_LOOP(session) (((Session*)session)->loop->event_loop)
#define TIMER_ACTIVE(timer) (timer == NO_ACTIVE_TIMER_ID)


typedef struct {
    PyObject_HEAD
    aeEventLoop *event_loop;
    PyThreadState* thread_state;
    bool stop;
    int req_in_read;
    int req_in_write;
    int req_out_read;
    int req_out_write;
} EventLoop;


typedef struct {
    PyObject_HEAD
    EventLoop *loop;
    CURLM *multi;
    CURLSH *shared;
    long long timer_id;
} Session;


typedef struct {
    PyObject *future;
    char *method;
    char *url;
    struct curl_slist *headers;
    char *auth;
    char *cookies;
    Py_buffer *req_data;
    Session *session;
} AcRequestStart;


struct BufferNode {
    int len;
    char *buffer;
    struct BufferNode *next;
};


typedef struct {
    PyObject *future;
    struct curl_slist *headers;
    CURL *curl;
    Session *session;
    Py_buffer *req_data;
    struct BufferNode *resp_buffer;
} AcActiveRequest;


typedef struct {
    PyObject *future;
    Session *session;
    Py_buffer *req_data;
    CURL *curl;
    CURLcode result;
    struct BufferNode *resp_buffer;
} AcCompleteRequest;


typedef struct {
    PyObject_HEAD
    struct BufferNode *resp_buffer;
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
    free_buffer_nodes(self->resp_buffer);
    curl_easy_cleanup(self->curl);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Response_get_raw(Response *self, PyObject *args)
{
	int i = 0, len = 0;
	PyObject* list;
	struct BufferNode *node = self->resp_buffer;
    while(node != NULL)
    {
		len++;
        node = node->next;
    };
	list = PyList_New(len);
	node = self->resp_buffer;
    while(node != NULL)
    {
        i++;
        PyList_SET_ITEM(list, len - i, PyBytes_FromStringAndSize(node->buffer, node->len));
        node = node->next;
    };
    //printf("Response_get_raw\n");
    return Py_BuildValue("O", list);
}

static PyObject *Response_get_response_code(Response *self, PyObject *args)
{
    long response_code;
    curl_easy_getinfo(self->curl, CURLINFO_RESPONSE_CODE, &response_code);
    return PyLong_FromLong(response_code);
}

static PyObject *Response_get_redirect_url(Response *self, PyObject *args)
{
    char *url = NULL;
    curl_easy_getinfo(self->curl, CURLINFO_REDIRECT_URL, &url);
    if(url != NULL) {
        return PyBytes_FromString(url);
    }
    else {
        Py_RETURN_NONE;
    }
}


static PyMethodDef Response_methods[] = {
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, "Get the redirect URL or None"},
    {"get_response_code", (PyCFunction)Response_get_response_code, METH_NOARGS, "Get the HTTP Response Code"},
    {"get_raw", (PyCFunction)Response_get_raw, METH_NOARGS, "Get the unprocessed response data"},
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
    int remaining_in_queue;
    AcActiveRequest *ar;
    CURLMsg *msg;
    while(1)
    {
        msg = curl_multi_info_read(session->multi, &remaining_in_queue);
        if(msg == NULL) {
            break;
        }
        AcCompleteRequest *cr = (AcCompleteRequest*)malloc(sizeof(AcCompleteRequest));
        curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (void **)&ar);
        cr->result = msg->data.result;
        cr->session = ar->session;
        cr->future = ar->future;
        cr->curl = ar->curl;
        cr->req_data = ar->req_data;
        cr->resp_buffer = ar->resp_buffer;
        curl_multi_remove_handle(ar->session->multi, ar->curl);
        //curl_easy_cleanup(ar->curl);
        curl_slist_free_all(ar->headers);
        free(ar);

        //printf("response_complete: writing to req_out_write\n");
        int wrote = write(session->loop->req_out_write, &cr, sizeof(AcCompleteRequest *));
        //printf("response_complete: done; wrote=%d\n", wrote);
    }
}


void socket_action_and_response_complete(Session *session, curl_socket_t socket, int ev_bitmask) 
{
    int running_handles;
    curl_multi_socket_action(session->multi, socket, ev_bitmask, &running_handles);
    //printf("socket_action_and_response_complete:  session_handles_after=%d\n", running_handles);
    response_complete(session);
}


size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    if(size * nmemb == 0) {
        return 0;
    }
    AcActiveRequest *ar = (AcActiveRequest *)userdata;
    struct BufferNode *node = (struct BufferNode *)malloc(sizeof(struct BufferNode));
    node->len = size * nmemb;
    node->buffer = (char*)malloc(node->len);
    memcpy(node->buffer, ptr, node->len);
    node->next = ar->resp_buffer;
    ar->resp_buffer = node;
    return node->len;
}
 

void start_request(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
{
    AcRequestStart *rs;
    EventLoop *loop = (EventLoop*)clientData;
    read(loop->req_in_read, &rs, sizeof(AcRequestStart *));
    if(rs == NULL)
    {
        return;
    }
    AcActiveRequest *ar = (AcActiveRequest*)malloc(sizeof(AcActiveRequest));
    //printf("do_request_start: read AcRequestStart\n");
    ar->curl = curl_easy_init();
    curl_easy_setopt(ar->curl, CURLOPT_URL, rs->url);
    curl_easy_setopt(ar->curl, CURLOPT_CUSTOMREQUEST, rs->method);
    if(rs->headers != NULL) {
        curl_easy_setopt(ar->curl, CURLOPT_HTTPHEADER, rs->headers);
    }
    if(rs->auth != NULL) {
        curl_easy_setopt(ar->curl, CURLOPT_USERPWD, rs->auth);
    }
    if(rs->cookies != NULL) {
        curl_easy_setopt(ar->curl, CURLOPT_COOKIE, rs->cookies);
    }
    if(rs->req_data != NULL) {
        curl_easy_setopt(ar->curl, CURLOPT_POSTFIELDSIZE, rs->req_data->len);
        curl_easy_setopt(ar->curl, CURLOPT_POSTFIELDS, (char*)rs->req_data->buf);
    }
    curl_easy_setopt(ar->curl, CURLOPT_PRIVATE, ar);
    curl_easy_setopt(ar->curl, CURLOPT_SHARE, rs->session->shared);
    curl_easy_setopt(ar->curl, CURLOPT_HEADER, 1L);
    //curl_easy_setopt(ar->curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(ar->curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(ar->curl, CURLOPT_WRITEDATA, ar);

    ar->future = rs->future;
    ar->headers = rs->headers;
    ar->session = rs->session;
    ar->req_data = rs->req_data;
    ar->resp_buffer = NULL;
    free(rs->method);
    free(rs->url);
    if(rs->auth != NULL) {
        free(rs->auth);
    }
    if(rs->cookies != NULL) {
        free(rs->cookies);
    }
    free(rs);
        
    curl_multi_add_handle(ar->session->multi, ar->curl);
    //printf("do_request_start: added handle\n");
    socket_action_and_response_complete(ar->session, CURL_SOCKET_TIMEOUT, 0);
    return;
}


static PyObject *
EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    EventLoop *self = (EventLoop *)type->tp_alloc(type, 0);
    int req_in[2];
    int req_out[2];
    if (self != NULL) {
        self->event_loop = aeCreateEventLoop(10000);
        pipe(req_in);
        self->req_in_read = req_in[0];
        self->req_in_write = req_in[1];
        pipe(req_out);
        self->req_out_read = req_out[0];
        self->req_out_write = req_out[1];
        if(aeCreateFileEvent(self->event_loop, self->req_in_read, AE_READABLE, start_request, self) == AE_ERR) {
            exit(1);
        }
    }
    return (PyObject *)self;
}


static void
EventLoop_dealloc(EventLoop *self)
{
    aeDeleteEventLoop(self->event_loop);
    close(self->req_in_read);
    close(self->req_in_write);
    close(self->req_out_read);
    close(self->req_out_write);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
EventLoop_main(EventLoop *self, PyObject *args)
{
    //printf("EventLoop_main: Started\n");
    self->thread_state = PyEval_SaveThread();
    do {
        //printf("EventLoop_main: Start of aeProcessEvents; has_events=%d\n", aeHasEvents(self->event_loop));
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS);
        //printf("EventLoop_main: End of aeProcessEvents; has_events=%d\n", aeHasEvents(self->event_loop));
    } while(!self->stop);
    PyEval_RestoreThread(self->thread_state);
    //printf("EventLoop_main: Ended; has_events=%d\n", aeHasEvents(self->event_loop));
    Py_RETURN_NONE;
}


static PyObject *
EventLoop_stop(PyObject *self, PyObject *args)
{
    static void *null = NULL;
    ((EventLoop*)self)->stop = true;
    //printf("EventLoop_stop: writing stop to req_in_write\n");
    write(((EventLoop*)self)->req_in_write, &null, sizeof(void *));
    Py_RETURN_NONE;
}

static PyObject *
Eventloop_get_out_fd(PyObject *self, PyObject *args)
{
    //printf("EventLoop_get_out_fd\n");
    return Py_BuildValue("i", ((EventLoop*)self)->req_out_read);
}


static PyObject *
Eventloop_get_completed(PyObject *self, PyObject *args)
{
    AcCompleteRequest *cr;
    PyObject *rtn;
    int nread = read(((EventLoop*)self)->req_out_read, &cr, sizeof(AcCompleteRequest *));
    //printf("EventLoop_get_completed_request: read RequestData; read=%d address=%p\n", nread, request);
    Py_XDECREF(cr->session);
    Py_XINCREF(Py_None);
    if(cr->result == CURLE_OK) {
        Response *response = PyObject_New(Response, (PyTypeObject *)&ResponseType);
        response->resp_buffer = cr->resp_buffer;
        response->curl = cr->curl;
        rtn = Py_BuildValue("OOO", Py_None, response, cr->future);
    }
    else {
        PyObject* error = PyUnicode_FromString(curl_easy_strerror(cr->result));
        free_buffer_nodes(cr->resp_buffer);
        curl_easy_cleanup(cr->curl);
        rtn = Py_BuildValue("OOO", error, Py_None, cr->future);
    }
    Py_XDECREF(cr->future);
    if(cr->req_data != NULL) {
        PyBuffer_Release(cr->req_data);
    }
    free(cr);
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
    //printf("socket_event\n");
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
    //printf("socket_callback: handle=%p socket=%d what=%d\n", easy, s, what);
    int result = 10; //FIXME fake value because of CURL_POLL_REMOVE case
    switch(what) {
        case CURL_POLL_NONE:
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_NONE, NULL, NULL);
            break;
        case CURL_POLL_IN:
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE, socket_event, userp);
            break;
        case CURL_POLL_OUT:
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_WRITABLE, socket_event, userp);
            break;
        case CURL_POLL_INOUT:
            result = aeCreateFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE | AE_WRITABLE, socket_event, userp);
            break;
        case CURL_POLL_REMOVE:
            aeDeleteFileEvent(SESSION_AE_LOOP(userp), (int)s, AE_READABLE | AE_WRITABLE);
            break;
    };
    if(result == AE_ERR) {
        //printf("socket_callback failed\n");
        exit(1);
    }
    return 0;
}




int timeout(struct aeEventLoop *eventLoop, long long id, void *clientData) 
{
    //printf("timeout\n");
    Session *session = (Session*)clientData;
    session->timer_id = NO_ACTIVE_TIMER_ID;
    socket_action_and_response_complete(session, CURL_SOCKET_TIMEOUT, 0);
    return AE_NOMORE;
}


int timer_callback(CURLM *multi, long timeout_ms, void *userp)
{
    //printf("timeout_callback: timeout_ms=%ld\n", timeout_ms);
    Session *session = (Session*)userp;
    if(session->timer_id != NO_ACTIVE_TIMER_ID) {
        //printf("timeout_callback: delete timer; session->timer_id=%ld\n", session->timer_id);
        aeDeleteTimeEvent(SESSION_AE_LOOP(session), session->timer_id);
        session->timer_id = NO_ACTIVE_TIMER_ID;
    }
    if(timeout_ms > 0) {
        if((session->timer_id = aeCreateTimeEvent(SESSION_AE_LOOP(session), timeout_ms, timeout, userp, NULL)) == AE_ERR) {
            //printf("timer_callback failed\n");
            exit(1);
        }
        //printf("timeout_callback: create timer; session->timer_id=%ld\n", session->timer_id);
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
    curl_multi_cleanup(self->multi);
    curl_share_cleanup(self->shared);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Session_request(Session *self, PyObject *args, PyObject *kwds)
{
    char a_pad[1024];
    memset(a_pad, 0, 1024);
    PyObject *future;
    char *method;
    char *url;
    PyObject *headers;
    char *auth;
    char *cookies;
    Py_buffer *req_data = NULL;
    static char *kwlist[] = {"future", "method", "url", "headers", "auth", "cookies", "data", NULL};
    char b_pad[1024];
    memset(b_pad, 0, 1024);

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OssO!zzz*", kwlist, &future, &method, &url, &PyTuple_Type, &headers, &auth, &cookies, &req_data)) {
        return NULL;
    }
    AcRequestStart *rs = (AcRequestStart *)malloc(sizeof(AcRequestStart));
    Py_INCREF(self);
    rs->session = self;
    Py_INCREF(future);
    rs->future = future;

    rs->method = strdup(method);
    rs->url = strdup(url);
    rs->headers = NULL;
    for(int i =0; i < PyTuple_GET_SIZE(headers); i++) {
        rs->headers = curl_slist_append(rs->headers, PyUnicode_AsUTF8(PyTuple_GET_ITEM(headers, i)));
    }
    
    rs->auth = auth != NULL ? strdup(auth) : NULL;
    rs->cookies = cookies != NULL ? strdup(cookies) : NULL;

    rs->req_data = req_data;

    write(self->loop->req_in_write, &rs, sizeof(AcRequestStart *));
    //printf("Session_request: scheduling request\n");
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


