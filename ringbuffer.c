#include "ringbuffer.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

#define M sizeof(int)
#define ALIGN(s) (((s) + M-1 ) & ~(M-1))

struct ringbuffer {
	int size;
	int head;      //head is sum of length of all blk
};

//get start address of given blk
static inline int
block_offset(struct ringbuffer * rb, struct ringbuffer_block * blk) {
	char * start = (char *)(rb + 1);
	return (char *)blk - start;
}

static inline struct ringbuffer_block *
block_ptr(struct ringbuffer * rb, int offset) {
	char * start = (char *)(rb + 1);                                   //jump over struct its self , to data seg [struct_self-data]
	return (struct ringbuffer_block *)(start + offset);
}

static inline struct ringbuffer_block *
block_next(struct ringbuffer * rb, struct ringbuffer_block * blk) {

	int align_length = ALIGN(blk->length);
	int head = block_offset(rb, blk);               //head is result of last blk - (start+1),head is relative address of blk
	if (align_length + head == rb->size) {
		return NULL;
	}
	assert(align_length + head < rb->size);
	return block_ptr(rb, head + align_length);      //offset of last bulk + length of last bulk
}

struct ringbuffer *
ringbuffer_new(int size) {
	struct ringbuffer * rb = malloc(sizeof(*rb) + size);

    printf("size of init rb is %d \n", sizeof(*rb));

	rb->size = size;
	rb->head = 0;
	struct ringbuffer_block * blk = block_ptr(rb, 0);   //get address of memory
	blk->length = size;
	blk->id = -1;
	return rb;
}

void
ringbuffer_delete(struct ringbuffer * rb) {
	free(rb);
}

void
ringbuffer_link(struct ringbuffer *rb , struct ringbuffer_block * head, struct ringbuffer_block * next) {
	//head blk already have a next blk, shift to this "next blk"
    while (head->next >=0) {
		head = block_ptr(rb, head->next);
	}
	//set 2 block of same id
	next->id = head->id;
    //set blk1 -> next offset of blk2
	head->next = block_offset(rb, next);
}

static struct ringbuffer_block *
_alloc(struct ringbuffer * rb, int total_size , int size) {

    //get start point ,from rb->head on
	struct ringbuffer_block * blk = block_ptr(rb, rb->head);
    //get align length
	int align_length = ALIGN(sizeof(struct ringbuffer_block) + size);
    //set real length
	blk->length = sizeof(struct ringbuffer_block) + size;     //blk real size ,no align
	blk->offset = 0;                                          //set length with no align ,cause padding space no need to read
	blk->next = -1;
	blk->id = -1;
    //get next blk
	struct ringbuffer_block * next = block_next(rb, blk);
	if (next) {
		rb->head = block_offset(rb, next);                    //set head to offset(start) of next blk
		if (align_length < total_size) {
			next->length = total_size - align_length;         //next blk is the remain space, set length of rest space
			if (next->length >= sizeof(struct ringbuffer_block)) {
				next->id = -1;  //-1 means blk available
			}
		}
	} else {
		rb->head = 0;                                         //if no next , means roll back ,head set to 0
	}
	return blk;
}


//ring buffer alloc
struct ringbuffer_block *
ringbuffer_alloc(struct ringbuffer * rb, int size) {

    //get align size needed
	int align_length = ALIGN(sizeof(struct ringbuffer_block) + size);
	int i;
	for (i=0;i<2;i++) {
		int free_size = 0;

        //find start pointer, from rb->head on
		struct ringbuffer_block * blk = block_ptr(rb, rb->head);    //blk is next point of last blk
                                                                    //so blk is next space to use
		do {
            //轮转之前不会执行,只有轮转后后可能执行,轮转后遇到不可分配的,应该强行回收
			if (blk->length >= sizeof(struct ringbuffer_block) && blk->id >= 0)    //id >= 0 means mem in use
				return NULL;
			free_size += ALIGN(blk->length);
			if (free_size >= align_length) {
				return _alloc(rb, free_size , size);
			}
			blk = block_next(rb, blk);              //find space available
		} while(blk);
		rb->head = 0;    //roll back ,do again
	}
	return NULL;
}

static int
_last_id(struct ringbuffer * rb) {
	int i;
	for (i=0;i<2;i++) {
		struct ringbuffer_block * blk = block_ptr(rb, rb->head);
		do {
			if (blk->length >= sizeof(struct ringbuffer_block) && blk->id >= 0)
				return blk->id;
			blk = block_next(rb, blk);
		} while(blk);
		rb->head = 0;
	}
	return -1;
}

int
ringbuffer_collect(struct ringbuffer * rb) {
	int id = _last_id(rb);
	struct ringbuffer_block *blk = block_ptr(rb, 0);
	do {
		if (blk->length >= sizeof(struct ringbuffer_block) && blk->id == id) {
			blk->id = -1;
		}
		blk = block_next(rb, blk);
	} while(blk);
	return id;
}

void
ringbuffer_shrink(struct ringbuffer * rb, struct ringbuffer_block * blk, int size) {
	if (size == 0) {
		rb->head = block_offset(rb, blk);
		return;
	}
	int align_length = ALIGN(sizeof(struct ringbuffer_block) + size);
	int old_length = ALIGN(blk->length);
	assert(align_length <= old_length);
	blk->length = size + sizeof(struct ringbuffer_block);
	if (align_length == old_length) {
		return;
	}
	blk = block_next(rb, blk);
	blk->length = old_length - align_length;
	if (blk->length >= sizeof(struct ringbuffer_block)) {
		blk->id = -1;
	}
	rb->head = block_offset(rb, blk);
}

static int
_block_id(struct ringbuffer_block * blk) {
	assert(blk->length >= sizeof(struct ringbuffer_block));
	int id = blk->id;
	assert(id>=0);
	return id;
}

//set id to -1 ,means it's free to alloc
void
ringbuffer_free(struct ringbuffer * rb, struct ringbuffer_block * blk) {
	if (blk == NULL)
		return;
	int id = _block_id(blk);
	blk->id = -1;
	while (blk->next >= 0) {
		blk = block_ptr(rb, blk->next);
		assert(_block_id(blk) == id);
		blk->id = -1;
	}
}

//todo ? skip is used to jump over given bytes?
int
ringbuffer_data(struct ringbuffer * rb, struct ringbuffer_block * blk, int size, int skip, void **ptr) {
	int length = blk->length - sizeof(struct ringbuffer_block) - blk->offset;
	for (;;) {
		if (length > skip) {
			if (length - skip >= size) {
				char * start = (char *)(blk + 1);
				*ptr = (start + blk->offset + skip);
				return size;
			}
            //set address to read to NULL
			*ptr = NULL;
			int ret = length - skip;
			while (blk->next >= 0) {
				blk = block_ptr(rb, blk->next);
				ret += blk->length - sizeof(struct ringbuffer_block);
				if (ret >= size)
					return size;
			}
			return ret;
		}
		if (blk->next < 0) {
			assert(length == skip);
			*ptr = NULL;
			return 0;
		}
		blk = block_ptr(rb, blk->next);
		assert(blk->offset == 0);
		skip -= length;
		length = blk->length - sizeof(struct ringbuffer_block);
	}
}


void *
ringbuffer_copy(struct ringbuffer * rb, struct ringbuffer_block * from, int skip, struct ringbuffer_block * to) {

    int size = to->length - sizeof(struct ringbuffer_block);
	int length = from->length - sizeof(struct ringbuffer_block) - from->offset;
	char * ptr = (char *)(to+1);
	for (;;) {
		if (length > skip) {    //data length bigger than skip ,need to copy
			char * src = (char *)(from + 1);
			src += from->offset + skip;
			length -= skip;         //length of data to copy
			while (length < size) {     //if small than size, pull from its next blk
				memcpy(ptr, src, length);
				assert(from->next >= 0);    //has a next blk
				from = block_ptr(rb , from->next);
				assert(from->offset == 0);  //if offset not 0 ,means this blk has data to be handle
				ptr += length;      //shift dest address
				size -= length;     //re calculate size to copy
				length = from->length - sizeof(struct ringbuffer_block);    //data length of blk
				src =  (char *)(from + 1);
			}
			memcpy(ptr, src , size);    //if data length is enough ,do copy
			to->id = from->id;      //copy id
			return (char *)(to + 1);        //return dest
		}
		assert(from->next >= 0);        //if length is not enough to skip ,shift to next blk ,skip the left num
		from = block_ptr(rb, from->next);
		assert(from->offset == 0);
		skip -= length;     //remaind skip
		length = from->length - sizeof(struct ringbuffer_block);        //get a new length of a new blk ,restart
	}
}

//get a blk needed ,with skip param
struct ringbuffer_block *
ringbuffer_yield(struct ringbuffer * rb, struct ringbuffer_block *blk, int skip) {
	int length = blk->length - sizeof(struct ringbuffer_block) - blk->offset;       //-offset 意思是处理未处理的数据,offset是未处理的数据起始点
	for (;;) {
		if (length > skip) {
			blk->offset += skip;    //shift offset by skip ,skip means no need to process
			return blk;
		}
		blk->id = -1;
		if (blk->next < 0) {
			return NULL;
		}
		blk = block_ptr(rb, blk->next);
		assert(blk->offset == 0);
		skip -= length;     //re calculate skip, skip is consumed partly by last blk
		length = blk->length - sizeof(struct ringbuffer_block);
	}
}

void 
ringbuffer_dump(struct ringbuffer * rb) {
	struct ringbuffer_block *blk = block_ptr(rb,0);
	int i=0;
	printf("total size= %d\n",rb->size);
	while (blk) {
		++i;
		if (i>10)
			break;
		if (blk->length >= sizeof(*blk)) {
			printf("[%u : %d]", (unsigned)(blk->length - sizeof(*blk)), block_offset(rb,blk));
			printf(" id=%d",blk->id);
			if (blk->id >=0) {
				printf(" offset=%d next=%d",blk->offset, blk->next);
			}
		} else {
			printf("<%u : %d>", blk->length, block_offset(rb,blk));
		}
		printf("\n");
		blk = block_next(rb, blk);
	}
}
