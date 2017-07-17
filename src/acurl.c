#include "ae/ae.h"
#include <curl/multi.h>
#include <Python.h>
#include "structmember.h"

#define NO_ACTIVE_TIMER_ID -1
#define MAX_COMPLETE_AT_ONCE 500
#define SESSION_AE_LOOP(session) (((Session*)session)->loop->event_loop)

typedef struct RequestData RequestData;


typedef struct {
    PyObject_HEAD
    aeEventLoop *event_loop;
    PyThreadState* thread_state;
    int complete_timer;
    RequestData *ready_to_complete_list[MAX_COMPLETE_AT_ONCE];
    int ready_to_complete_len;
    PyObject *complete_py_callback;
    int running_handles;
} EventLoop;


typedef struct {
    PyObject_HEAD
    EventLoop *loop;
    CURLM *multi;
    CURLSH *shared;
    long long timer_id;
    int running_handles;
} Session;


struct RequestData{
    PyObject_HEAD
    char *url;
    Session *session;
    CURL *curl;
    PyObject *user_object;
    CURLcode result;
};


static void
RequestData_dealloc(RequestData *self)
{
    Py_DECREF(self->session);
    Py_DECREF(self->user_object);
    curl_easy_cleanup(self->curl);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyMemberDef RequestData_members[] = {
    {"user_object", T_OBJECT_EX, offsetof(RequestData, user_object), READONLY, ""},
    {NULL}
};


static PyTypeObject RequestDataType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "acurl.RequestData",           /* tp_name */
    sizeof(RequestData),           /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)RequestData_dealloc,    /* tp_dealloc */
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
    "Request Data Type",                        /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    0,                         /* tp_methods */
    RequestData_members,                         /* tp_members */
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


int alive(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    //printf("*** AE ALIVE\n");
    return 1000;
}


static PyObject *
EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    EventLoop *self = NULL;
    PyObject *completion_callback;
    static char *kwlist[] = {"completion_callback", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &completion_callback)) {
        self = (EventLoop *)type->tp_alloc(type, 0);       
        if (self != NULL) {
            self->event_loop = aeCreateEventLoop(10000);
            self->complete_timer = NO_ACTIVE_TIMER_ID;
            Py_INCREF(completion_callback);
            self->complete_py_callback = completion_callback;
        }
        aeCreateTimeEvent(self->event_loop, 1000, alive, NULL, NULL);
    }
    return (PyObject *)self;
}


static void
EventLoop_dealloc(EventLoop *self)
{
    aeDeleteEventLoop(self->event_loop);
    Py_XDECREF(self->complete_py_callback);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
EventLoop_main(EventLoop *self, PyObject *args)
{
	self->thread_state = PyEval_SaveThread();
    //printf("EventLoop_main\n");
    do {
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS | AE_DONT_WAIT); 
    } while(self->running_handles > 0);
    //printf("EventLoop_main End\n");
    PyEval_RestoreThread(self->thread_state);
    Py_RETURN_NONE;
}


static PyObject *
EventLoop_stop(PyObject *self, PyObject *args)
{
    aeStop(((EventLoop*)self)->event_loop);
    Py_RETURN_NONE;
}


static PyMethodDef EventLoop_methods[] = {
    {"main", (PyCFunction)EventLoop_main, METH_VARARGS, "Run the event loop"},
    {"stop", EventLoop_stop, METH_VARARGS, "Stop the event loop"},
    {NULL, NULL, 0, NULL}
};


static PyMemberDef EventLoop_members[] = {
    {"_completion_callback", T_OBJECT_EX, offsetof(EventLoop, complete_py_callback), READONLY, "Called with a list of failed of successful requests, set via constructor"},
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


void complete_py_callback(EventLoop *loop) {
    int i;
    PyObject *list;
    PyEval_RestoreThread(loop->thread_state);
    list = PyList_New(loop->ready_to_complete_len);
    for(i = 0; i < loop->ready_to_complete_len; i++) {
        PyList_SET_ITEM(list, i, (PyObject*)loop->ready_to_complete_list[i]);
        //if(request_data->result == CURLE_OK) {
        //PyObject *error_str = PyUnicode_FromString(curl_easy_strerror(request_data->result));
    }
    PyObject *args = PyTuple_Pack(1, list);

    PyObject_CallObject(loop->complete_py_callback, args);
    //printf("Running handles complete_py_callback before %d\n", loop->running_handles);
    loop->running_handles -= loop->ready_to_complete_len;
    //printf("Running handles complete_py_callback after %d\n", loop->running_handles);
    loop->thread_state = PyEval_SaveThread();
    loop->ready_to_complete_len = 0;
}


int complete_py_callback_timed(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    complete_py_callback((EventLoop *)clientData);
    ((EventLoop *)clientData)->complete_timer = NO_ACTIVE_TIMER_ID;
    return AE_NOMORE;
}


void response_complete(Session *session) 
{
    int remaining_in_queue;
    CURLMsg *msg;
    if(session->loop->complete_timer == NO_ACTIVE_TIMER_ID) {
        session->loop->complete_timer = aeCreateTimeEvent(SESSION_AE_LOOP(session), 0, complete_py_callback_timed, session->loop, NULL);
    }
    while(1)
    {
        msg = curl_multi_info_read(session->multi, &remaining_in_queue);
        if(msg == NULL) {
            break;
        }
        curl_multi_remove_handle(session->multi, msg->easy_handle);
        curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (void **)&(session->loop->ready_to_complete_list[session->loop->ready_to_complete_len]));
        session->loop->ready_to_complete_list[session->loop->ready_to_complete_len]->result = msg->data.result;
        session->loop->ready_to_complete_len++;
        if(session->loop->ready_to_complete_len == MAX_COMPLETE_AT_ONCE) {
            if(session->loop->complete_timer != NO_ACTIVE_TIMER_ID) {
                aeDeleteTimeEvent(SESSION_AE_LOOP(session), session->loop->complete_timer);
                session->loop->complete_timer = NO_ACTIVE_TIMER_ID;
            }
            complete_py_callback(session->loop);
        }
    }
}


void socket_action_and_response_complete(Session *session, curl_socket_t socket, int ev_bitmask) 
{
    //printf("socket_action_and_response_complete\n");
    int running_handles;
    curl_multi_socket_action(session->multi, socket, ev_bitmask, &running_handles);
    //printf("socket_action_and_response_complete2 %d %d\n", running_handles, session->running_handles);
    if(running_handles < session->running_handles) {
        response_complete(session);
        session->running_handles = running_handles;
    }
}


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
    //printf("socket_callback %p %d %d\n", easy, s, what);
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
    //printf("timeout_callback %ld\n", timeout_ms);
    Session *session = (Session*)userp;
    if(session->timer_id != NO_ACTIVE_TIMER_ID) {
        aeDeleteTimeEvent(SESSION_AE_LOOP(session), session->timer_id);
        session->timer_id = NO_ACTIVE_TIMER_ID;
    }
    if(timeout_ms > 0) {
        if((session->timer_id = aeCreateTimeEvent(SESSION_AE_LOOP(session), timeout_ms, timeout, userp, NULL)) == AE_ERR) {
            //printf("timer_callback failed\n");
            exit(1);
        }
        //printf("timeout registered\n");
    }
    else if(timeout_ms == 0) {
        //printf("timeout callback immediate\n");
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


size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    return size * nmemb;
}
 

int do_request_start(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    //printf("do_request_start\n");
    RequestData *request_data = (RequestData *)clientData;
    CURL *curl = request_data->curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, request_data->url);
    free(request_data->url);
    request_data->url = NULL;
    curl_easy_setopt(curl, CURLOPT_PRIVATE, request_data);
    curl_easy_setopt(curl, CURLOPT_SHARE, request_data->session->shared);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    request_data->session->running_handles++;
    curl_multi_add_handle(request_data->session->multi, curl);
    //printf("DONE do_request_start %p\n", curl);
    return AE_NOMORE;
}


static PyObject *
Session_request(PyObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"url", "user_object", NULL};
    char *url;
    PyObject *user_object;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sO", kwlist, &url, &user_object)) {
        return NULL;
    }
    RequestData *request_data = PyObject_New(RequestData, (PyTypeObject *)&RequestDataType);
    Py_INCREF(self);
    request_data->session = (Session *)self;
    request_data->session->loop->running_handles++;
    //printf("Running handles Session_request %d\n", request_data->session->loop->running_handles);
    request_data->url = strdup(url);
    Py_INCREF(user_object);
    request_data->user_object = user_object;
    aeCreateTimeEvent(SESSION_AE_LOOP(self), 0, do_request_start, request_data, NULL);
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

    if (PyType_Ready(&RequestDataType) < 0)
        return NULL;

    m = PyModule_Create(&_acurl_module);

    if(m != NULL) {
        curl_global_init(CURL_GLOBAL_ALL); // init curl library
        Py_INCREF(&SessionType);
        PyModule_AddObject(m, "Session", (PyObject *)&SessionType);
        Py_INCREF(&EventLoopType);
        PyModule_AddObject(m, "EventLoop", (PyObject *)&EventLoopType);
        Py_INCREF(&RequestDataType);
        PyModule_AddObject(m, "RequestData", (PyObject *)&RequestDataType);
    }
    
    return m;
}


