#include "ae/ae.h"
#include <curl/multi.h>
#include <Python.h>
#include <pthread.h>
#include "structmember.h"

#define NO_ACTIVE_TIMER_ID -1
#define SESSION_AE_LOOP(session) (((Session*)session)->loop->event_loop)
#define TIMER_ACTIVE(timer) (timer == NO_ACTIVE_TIMER_ID)
#define TIMER_DELETE(timer) (timer



typedef struct RequestData RequestData;


typedef struct RequestData_node {
    RequestData request_data;
    RequestData_node* next;
} RequestData_node;


typedef struct {
    PyObject_HEAD
    aeEventLoop *event_loop;
    PyThreadState* thread_state;
    int complete_timer;
    PyObject *complete_py_callback;
    RequestData_node *ready_to_complete_head;
    RequestData_node *new_requests_head;
    pthread_cond_t new_requests_cond;
    pthread_mutex_t new_requests_mutex
} EventLoop;


typedef struct {
    PyObject_HEAD
    EventLoop *loop;
    CURLM *multi;
    CURLSH *shared;
    long long timer_id;
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


void free_RequestData_list(RequestData_node *head) {
    RequestData_node *next;
    RequestData_node *curr = head;
    while(curr) {
        next = curr->next;
        free(curr);
        curr = next;
    }
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
            self->new_requests_mutex = PTHREAD_MUTEX_INITIALIZER;
            self->new_requests_cond = PTHREAD_COND_INITIALIZER;
            self->ready_to_complete_head = NULL;
            self->new_requests_head = NULL;
            self->event_loop = aeCreateEventLoop(10000);
            self->complete_timer = NO_ACTIVE_TIMER_ID;
            Py_INCREF(completion_callback);
            self->complete_py_callback = completion_callback;
        }
    }
    return (PyObject *)self;
}

static void
EventLoop_dealloc(EventLoop *self)
{
    aeDeleteEventLoop(self->event_loop);
    free_RequestData_list(self->ready_to_complete_head);
    free_RequestData_list(self->new_requests_head);
    Py_XDECREF(self->complete_py_callback);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

void session_timeout(Session *session) {
    int running_handles;
    curl_multi_socket_action(session->multi, CURL_SOCKET_TIMEOUT, 0, &running_handles);
}

void start_new_requests(RequestData_node *node) {
	Session *prev_session = NULL;
	while(node != NULL) {
        RequestData_node *next;
        RequestData *request_data = node->request_data;
        CURL *curl = request_data->curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_URL, request_data->url);
        free(request_data->url);
        request_data->url = NULL;
        curl_easy_setopt(curl, CURLOPT_PRIVATE, request_data);
        curl_easy_setopt(curl, CURLOPT_SHARE, request_data->session->shared);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_multi_add_handle(request_data->session->multi, curl);
        //printf("start_new_requests: added handle\n");
        if(session != request_data->session && session != NULL) {
            session_timeout(prev_session);
        }
        prev_session = request_data->session;
        next = node->next;
        free(node);
        node = next;
    };
    if(prev_session != NULL) {
        session_timeout(prev_session);
    }
}

void start_new_requests_or_wait(EventLoop *self) {
    RequestData_node *node;
    pthread_mutex_lock(&self->new_requests_mutex);
    if(!aeHasEvents(self->event_loop) && self->new_requests_head == NULL) {
        pthread_cond_wait(&self->new_requests_cond, &self->new_requests_mutex);
    }
    node = self->new_requests_head;
    self->new_requests_head = NULL;
    pthread_mutex_unlock(&self->new_requests_mutex);
    start_new_requests(node);
}

static PyObject *
EventLoop_main(EventLoop *self, PyObject *args)
{
    //printf("EventLoop_main: Started\n");
    self->thread_state = PyEval_SaveThread();
    do {
        //printf("EventLoop_main: Start of aeProcessEvents; has_events=%d\n", aeHasEvents(self->event_loop));
        start_new_requests_or_wait(self);
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS | AE_DONT_WAIT); 
        //printf("EventLoop_main: End of aeProcessEvents; has_events=%d\n", aeHasEvents(self->event_loop));
    } while(1);
    PyEval_RestoreThread(self->thread_state);
    //printf("EventLoop_main: Grabbed GIL; has_events=%d\n", aeHasEvents(self->event_loop));
    //printf("EventLoop_main: Ended; has_events=%d\n", aeHasEvents(self->event_loop));
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
    pthread_mutex_unlock(&loop->modification_lock);
    PyEval_RestoreThread(loop->thread_state);
    list = PyList_New(loop->ready_to_complete_len);
    for(i = 0; i < loop->ready_to_complete_len; i++) {
        PyList_SET_ITEM(list, i, (PyObject*)loop->ready_to_complete_list[i]);
        //if(request_data->result == CURLE_OK) {
        //PyObject *error_str = PyUnicode_FromString(curl_easy_strerror(request_data->result));
    }
    PyObject *args = PyTuple_Pack(1, list);

    PyObject_CallObject(loop->complete_py_callback, args);
    //printf("complete_py_callback: number_event_loop_handles_removed=%d\n", loop->ready_to_complete_len);
    loop->thread_state = PyEval_SaveThread();
    pthread_mutex_lock(&loop->modification_lock);
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
        if(session->loop->ready_to_complete_len == READY_TO_COMPLETE_MAX) {
            complete_py_callback(session->loop);
        }
    }
    if(session->loop->ready_to_complete_len > 0 && session->loop->complete_timer == NO_ACTIVE_TIMER_ID) {
        session->loop->complete_timer = aeCreateTimeEvent(SESSION_AE_LOOP(session), 0, complete_py_callback_timed, session->loop, NULL);
    }
}


void socket_action_and_response_complete(Session *session, curl_socket_t socket, int ev_bitmask) 
{
    int running_handles;
    curl_multi_socket_action(session->multi, socket, ev_bitmask, &running_handles);
    //printf("socket_action_and_response_complete:  session_handles_after=%d\n", running_handles);
    response_complete(session);
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


size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    return size * nmemb;
}
 

int do_request_start(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    RequestData *request_data = (RequestData *)clientData;
    CURL *curl = request_data->curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, request_data->url);
    free(request_data->url);
    request_data->url = NULL;
    curl_easy_setopt(curl, CURLOPT_PRIVATE, request_data);
    curl_easy_setopt(curl, CURLOPT_SHARE, request_data->session->shared);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_multi_add_handle(request_data->session->multi, curl);
    //printf("do_request_start: added handle\n");
    socket_action_and_response_complete(request_data->session, CURL_SOCKET_TIMEOUT, 0);
    return AE_NOMORE;
}


static PyObject *
Session_request(PyObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"url", "user_object", NULL};
    char *url;
    PyObject *user_object;
    Session *session = (Session *)self;
    EventLoop *eventloop = session->loop;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sO", kwlist, &url, &user_object)) {
        return NULL;
    }
    RequestData *request_data = PyObject_New(RequestData, (PyTypeObject *)&RequestDataType);
    Py_INCREF(self);
    request_data->session = session;
    request_data->url = strdup(url);
    Py_INCREF(user_object);
    request_data->user_object = user_object;
    pthread_mutex_lock(&eventloop->modification_lock);
    aeCreateTimeEvent(SESSION_AE_LOOP(self), 0, do_request_start, request_data, NULL);
    pthread_mutex_unlock(&eventloop->modification_lock);
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


