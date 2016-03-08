#include "mread.h"
#include "ringbuffer.h"

/* Test for polling API */
#ifdef __linux__
#define HAVE_EPOLL 1
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#if !defined(HAVE_EPOLL) && !defined(HAVE_KQUEUE)
#error "system does not support epoll or kqueue API"
#endif
/* ! Test for polling API */

#ifdef HAVE_EPOLL
#include <sys/epoll.h>
#elif HAVE_KQUEUE
#include <sys/event.h>
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>

#define BACKLOG 32
#define READQUEUE 32
#define READBLOCKSIZE 2048
#define RINGBUFFER_DEFAULT 1024 * 1024


//socket status
#define SOCKET_INVALID 0
#define SOCKET_CLOSED 1
#define SOCKET_SUSPEND 2
#define SOCKET_READ 3
#define SOCKET_POLLIN 4

#define SOCKET_ALIVE	SOCKET_SUSPEND

//cast ~0 to intptr_t , intptr was introduced in c99, hold all pointer
#define LISTENSOCKET (void *)((intptr_t)~0)

//socket
struct socket {
	int fd;
	struct ringbuffer_block * node;
	struct ringbuffer_block * temp;
	int status;
};

//pool
struct mread_pool {

	int listen_fd;
#ifdef HAVE_EPOLL
	int epoll_fd;
#elif HAVE_KQUEUE
	int kqueue_fd;
#endif
	int max_connection;
	int closed;
	int active;                      //number of currently using socket
	int skip;
	struct socket * sockets;
	struct socket * free_socket;
    //length and head of kernel queue
	int queue_len;
	int queue_head;

#ifdef HAVE_EPOLL
	struct epoll_event ev[READQUEUE];
#elif HAVE_KQUEUE
	struct kevent ev[READQUEUE];     //event
#endif
	struct ringbuffer * rb;          //ring buffer
};

//create socket
static struct socket *
_create_sockets(int max) {

    printf("create sockets \n");

	int i;
	struct socket * s = malloc(max * sizeof(struct socket));
	for (i=0;i<max;i++) {             //make self sockets a linkedlist
		s[i].fd = i+1;
		s[i].node = NULL;
		s[i].temp = NULL;
		s[i].status = SOCKET_INVALID;
	}
	s[max-1].fd = -1;                 //todo  ?

    int j;
    for(j=0;j<max;j++){
        printf("fd of s [%d] is %d \n",j,s[j].fd);
    }

	return s;
}

//create ring buffer
static struct ringbuffer *
_create_rb(int size) {
	size = (size + 3) & ~3;
	if (size < READBLOCKSIZE * 2) {
		size = READBLOCKSIZE * 2;
	}
	struct ringbuffer * rb = ringbuffer_new(size);

	return rb;
}

//release ring buffer
static void
_release_rb(struct ringbuffer * rb) {
	ringbuffer_delete(rb);
}

//set socket to non blocking
static int
_set_nonblocking(int fd)
{
	int flag = fcntl(fd, F_GETFL, 0);
	if ( -1 == flag ) {
		return -1;
	}

	return fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

//create pool
//init socket
//init kqueue
//init self
struct mread_pool *
mread_create(int port , int max , int buffer_size) {
    //get fd
	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd == -1) {
		return NULL;
	}

    //set non block
	if ( -1 == _set_nonblocking(listen_fd) ) {
		return NULL;
	}

    //set reuse
	int reuse = 1;
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(int));

    //init host,port
	struct sockaddr_in my_addr;
	memset(&my_addr, 0, sizeof(struct sockaddr_in));
	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(port);
	my_addr.sin_addr.s_addr = htonl(INADDR_ANY); // INADDR_LOOPBACK

	printf("MREAD bind %s:%u\n",inet_ntoa(my_addr.sin_addr),ntohs(my_addr.sin_port));

    //bind
	if (bind(listen_fd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
		close(listen_fd);
		return NULL;
	}
    //listen
	if (listen(listen_fd, BACKLOG) == -1) {
		close(listen_fd);
		return NULL;
	}

#ifdef HAVE_EPOLL
	int epoll_fd = epoll_create(max + 1);
	if (epoll_fd == -1) {
		close(listen_fd);
		return NULL;
	}

	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = LISTENSOCKET;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
		close(listen_fd);
		close(epoll_fd);
		return NULL;
	}
#elif HAVE_KQUEUE
	int kqueue_fd = kqueue();	//init kqueue
	if (kqueue_fd == -1) {
		close(listen_fd);
		return NULL;
	}

	struct kevent ke;	//init kevent
	EV_SET(&ke, listen_fd, EVFILT_READ, EV_ADD, 0, 0, LISTENSOCKET);	//initializing a kevent structure

    //first register, register change to triger , no event
	if (kevent(kqueue_fd, &ke, 1, NULL, 0, NULL) == -1) {
		close(listen_fd);
		close(kqueue_fd);
		return NULL;
	}
#endif

    //init self
	struct mread_pool * self = malloc(sizeof(*self));
	self->listen_fd = listen_fd;

#ifdef HAVE_EPOLL
	self->epoll_fd = epoll_fd;
#elif HAVE_KQUEUE
	self->kqueue_fd = kqueue_fd;
#endif
	self->max_connection = max;
	self->closed = 0;
	self->active = -1;
	self->skip = 0;
	self->sockets = _create_sockets(max);            //create sockets
	self->free_socket = &self->sockets[0];           //free socket(could be used) is the first of sockets available

    printf("fd of free socket is %d \n",self->free_socket->fd);

	self->queue_len = 0;
	self->queue_head = 0;
	if (buffer_size == 0) {
		self->rb = _create_rb(RINGBUFFER_DEFAULT);   //create ring buffer
	} else {
		self->rb = _create_rb(buffer_size);
	}

	return self;
}



//close pool
void
mread_close(struct mread_pool *self) {
	if (self == NULL)
		return;
	int i;
	struct socket * s = self->sockets;

    //close all connection
	for (i=0;i<self->max_connection;i++) {
		if (s[i].status >= SOCKET_ALIVE) {
			close(s[i].fd);
		}
	}

	free(s);
	if (self->listen_fd >= 0) {
		close(self->listen_fd);
	}
#ifdef HAVE_EPOLL
	close(self->epoll_fd);
#elif HAVE_KQUEUE
	close(self->kqueue_fd);
#endif
	_release_rb(self->rb);
	free(self);
}

//return number of events
static int
_read_queue(struct mread_pool * self, int timeout) {

	self->queue_head = 0;

#ifdef HAVE_EPOLL
	int n = epoll_wait(self->epoll_fd , self->ev, READQUEUE, timeout);
#elif HAVE_KQUEUE
	struct timespec timeoutspec;
	timeoutspec.tv_sec = timeout / 1000;
	timeoutspec.tv_nsec = (timeout % 1000) * 1000000;

    //second register , just register event , change registered above
	int n = kevent(self->kqueue_fd, NULL, 0, self->ev, READQUEUE, &timeoutspec);
#endif
	if (n == -1) {
		self->queue_len = 0;
		return -1;
	}
	self->queue_len = n;
	return n;
}

//return data, a socket struct
inline static struct socket *
_read_one(struct mread_pool * self) {

	if (self->queue_head >= self->queue_len) {             // queue is empty or reach the end, todo, will head exceed len?
		return NULL;
	}
#ifdef HAVE_EPOLL
	return self->ev[self->queue_head ++].data.ptr;
#elif HAVE_KQUEUE
	return self->ev[self->queue_head ++].udata;            //get data of event
#endif
}


//alloc socket
static struct socket *
_alloc_socket(struct mread_pool * self) {

    printf("alloc socket... \n");

	if (self->free_socket == NULL) {
		return NULL;
	}
	struct socket * s = self->free_socket;

	int next_free = s->fd;                                 //fd point to next socket,fd of s [0] is 1 ,fd of s [1] is 2


    printf("next_free is %d \n",next_free);

	if (next_free < 0 ) {
		self->free_socket = NULL;
	} else {
		self->free_socket = &self->sockets[next_free];     //shift free socket to next one
	}

	return s;
}

//add client, assign fd to a free socket,which is a struct
static void
_add_client(struct mread_pool * self, int fd) {

    printf("add client... \n");

    //get one socket instant
	struct socket * s = _alloc_socket(self);
	if (s == NULL) {
        printf("no free socket ,return NULL \n");
		close(fd);
		return;
	}
#ifdef HAVE_EPOLL
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = s;
	if (epoll_ctl(self->epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
		close(fd);
		return;
	}
#elif HAVE_KQUEUE
	struct kevent ke;
	EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, s);
	if (kevent(self->kqueue_fd, &ke, 1, NULL, 0, NULL) == -1) {        //register change for socket
		close(fd);
		return;
	}
#endif

	s->fd = fd;
	s->node = NULL;
	s->status = SOCKET_SUSPEND;
}

static int
_report_closed(struct mread_pool * self) {
	int i;
	for (i=0;i<self->max_connection;i++) {
		if (self->sockets[i].status == SOCKET_CLOSED) {     //find a closed socket
			self->active = i;   //return its index
			return i;
		}
	}
	assert(0);
	return -1;
}


//poll event ,get socket id
int
mread_poll(struct mread_pool * self , int timeout) {

//    printf("mread poll start... \n");

	self->skip = 0;                                     //todo ?

//    printf(" active is %d : \n",self->active);

	if (self->active >= 0) {

		struct socket * s = &self->sockets[self->active];
		if (s->status == SOCKET_READ) {
			return self->active;
		}
	}
	if (self->closed > 0 ) {
		return _report_closed(self);
	}
	if (self->queue_head >= self->queue_len) {
		if (_read_queue(self, timeout) == -1) {

            printf("set self active \n");

			self->active = -1;
			return -1;
		}
	}

	//start polling
	for (;;) {

//        printf("in loop \n");

		struct socket * s = _read_one(self);
		if (s == NULL) {
			self->active = -1;
			return -1;
		}
		if (s == LISTENSOCKET) {    //new socket conn

            printf("LISTENSOCKET \n");

			struct sockaddr_in remote_addr;
			socklen_t len = sizeof(struct sockaddr_in);

            //accept
			int client_fd = accept(self->listen_fd , (struct sockaddr *)&remote_addr ,  &len);

            //print result
			if (client_fd >= 0) {
				printf("MREAD connect %s:%u (fd=%d)\n",inet_ntoa(remote_addr.sin_addr),ntohs(remote_addr.sin_port), client_fd);
				_add_client(self, client_fd);
			}
		} else {    //new data

            printf("not LISTENSOCKET \n");

			int index = s - self->sockets;             //get offset of 's' to address of sockets array

			assert(index >=0 && index < self->max_connection);
			self->active = index;

            printf("self active is %d \n",self->active);

			s->status = SOCKET_POLLIN;
			return index;
		}
	}
}

int
mread_socket(struct mread_pool * self, int index) {
	return self->sockets[index].fd;
}

static void
_link_node(struct ringbuffer * rb, int id, struct socket * s , struct ringbuffer_block * blk) {
	if (s->node) {
		ringbuffer_link(rb, s->node , blk);
	} else {
		blk->id = id;
		s->node = blk;
	}
}

void
mread_close_client(struct mread_pool * self, int id) {
	struct socket * s = &self->sockets[id];
	s->status = SOCKET_CLOSED;
	s->node = NULL;
	s->temp = NULL;
	close(s->fd);
	printf("MREAD close %d (fd=%d)\n",id,s->fd);

#ifdef HAVE_EPOLL
	epoll_ctl(self->epoll_fd, EPOLL_CTL_DEL, s->fd , NULL);
#elif HAVE_KQUEUE
	struct kevent ke;
	EV_SET(&ke, s->fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
	kevent(self->kqueue_fd, &ke, 1, NULL, 0, NULL);
#endif

	++self->closed;
}

static void
_close_active(struct mread_pool * self) {
	int id = self->active;
	struct socket * s = &self->sockets[id];
	ringbuffer_free(self->rb, s->temp);
	ringbuffer_free(self->rb, s->node);
	mread_close_client(self, id);
}

static char *
_ringbuffer_read(struct mread_pool * self, int *size) {
	struct socket * s = &self->sockets[self->active];
	if (s->node == NULL) {
		*size = 0;
		return NULL;
	}
	int sz = *size;
	void * ret;
	*size = ringbuffer_data(self->rb, s->node, sz , self->skip, &ret);
	return ret;
}


//get data
void *
mread_pull(struct mread_pool * self , int size) {

    printf("mread_pull ...\n");

	if (self->active == -1) {
		return NULL;
	}
	struct socket *s = &self->sockets[self->active];       //get current active socket

	int rd_size = size;                                    //read size
	char * buffer = _ringbuffer_read(self, &rd_size);
	if (buffer) {                                          //if buffer read

        printf("buffer read \n");

		self->skip += size;
		return buffer;
	}

    printf("buffer not read \n");

	switch (s->status) {                                   //if buffer not read
	case SOCKET_READ:
		s->status = SOCKET_SUSPEND;
	case SOCKET_CLOSED:
	case SOCKET_SUSPEND:
		return NULL;
	default:
		assert(s->status == SOCKET_POLLIN);
		break;
	}


	int sz = size - rd_size;	//sz is size to read
	int rd = READBLOCKSIZE;
	if (rd < sz) {
		rd = sz;
	}

	int id = self->active;
	struct ringbuffer * rb = self->rb;

	struct ringbuffer_block * blk = ringbuffer_alloc(rb , rd);
	while (blk == NULL) {
		int collect_id = ringbuffer_collect(rb);
		mread_close_client(self , collect_id);
		if (id == collect_id) {
			return NULL;
		}
		blk = ringbuffer_alloc(rb , rd);
	}

	buffer = (char *)(blk + 1);

	for (;;) {
		int bytes = recv(s->fd, buffer, rd, MSG_DONTWAIT);	//read bytes
		if (bytes > 0) {
			ringbuffer_shrink(rb, blk , bytes);
			if (bytes < sz) {
				_link_node(rb, self->active, s , blk);
				s->status = SOCKET_SUSPEND;     //shift status,cuz byte not full 4
				return NULL;
			}
			s->status = SOCKET_READ;
			break;
		}
		if (bytes == 0) {
			ringbuffer_shrink(rb, blk, 0);
			_close_active(self);
			return NULL;
		}
		if (bytes == -1) {
			switch(errno) {
			case EWOULDBLOCK:
				ringbuffer_shrink(rb, blk, 0);
				s->status = SOCKET_SUSPEND;
				return NULL;
			case EINTR:
				continue;
			default:
				ringbuffer_shrink(rb, blk, 0);
				_close_active(self);
				return NULL;
			}
		}
	}

	_link_node(rb, self->active , s , blk);

	void * ret;
	int real_rd = ringbuffer_data(rb, s->node , size , self->skip, &ret);
	if (ret) {
		self->skip += size;
		return ret;     //return data address
	}

    //ret null, real_rd >0 ,说明外部请求数据块在blk上不连续
	assert(real_rd == size);
	struct ringbuffer_block * temp = ringbuffer_alloc(rb, size);
	while (temp == NULL) {
		int collect_id = ringbuffer_collect(rb);
		mread_close_client(self , collect_id);
		if (id == collect_id) {
			return NULL;
		}
		temp = ringbuffer_alloc(rb , size);
	}
	temp->id = id;
	if (s->temp) {
		ringbuffer_link(rb, temp, s->temp);
	}
	s->temp = temp;
	ret = ringbuffer_copy(rb, s->node, self->skip, temp);
	assert(ret);
	self->skip += size;

	return ret;
}

void
mread_yield(struct mread_pool * self) {
	if (self->active == -1) {
		return;
	}
	struct socket *s = &self->sockets[self->active];
	ringbuffer_free(self->rb , s->temp);
	s->temp = NULL;
	if (s->status == SOCKET_CLOSED && s->node == NULL) {
		--self->closed;
		s->status = SOCKET_INVALID;
		s->fd = self->free_socket - self->sockets;
		self->free_socket = s;
		self->skip = 0;
		self->active = -1;
	} else {
		if (s->node) {
			s->node = ringbuffer_yield(self->rb, s->node, self->skip);
		}
		self->skip = 0;
		if (s->node == NULL) {
			self->active = -1;
		}
	}
}

int
mread_closed(struct mread_pool * self) {
	if (self->active == -1) {
		return 0;
	}
	struct socket * s = &self->sockets[self->active];
	if (s->status == SOCKET_CLOSED && s->node == NULL) {
		mread_yield(self);
		return 1;
	}
	return 0;
}
