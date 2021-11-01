/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_UTIL_QUEUE_H
#define _TD_UTIL_QUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

/*

This set of API for queue is designed specially for vnode/mnode. The main purpose is to 
consume all the items instead of one item from a queue by one single read. Also, it can 
combine multiple queues into a queue set, a consumer thread can consume a queue set via 
a single API instead of looping every queue by itself.

Notes:
1: taosOpenQueue/taosCloseQueue, taosOpenQset/taosCloseQset is NOT multi-thread safe 
2: after taosCloseQueue/taosCloseQset is called, read/write operation APIs are not safe.
3: read/write operation APIs are multi-thread safe

To remove the limitation and make this set of queue APIs multi-thread safe, REF(tref.c)
shall be used to set up the protection. 

*/

typedef void *taos_queue;
typedef void *taos_qset;
typedef void *taos_qall;
typedef void *(*FProcessItem)(void *pItem, void *ahandle);
typedef void *(*FProcessItems)(taos_qall qall, int numOfItems, void *ahandle);

taos_queue taosOpenQueue();
void       taosCloseQueue(taos_queue);
void       taosSetQueueFp(taos_queue, FProcessItem, FProcessItems);
void      *taosAllocateQitem(int size);
void       taosFreeQitem(void *pItem);
int        taosWriteQitem(taos_queue, void *pItem);
int        taosReadQitem(taos_queue, void **pItem);

taos_qall  taosAllocateQall();
void       taosFreeQall(taos_qall);
int        taosReadAllQitems(taos_queue, taos_qall);
int        taosGetQitem(taos_qall, void **pItem);
void       taosResetQitems(taos_qall);

taos_qset  taosOpenQset();
void       taosCloseQset();
void       taosQsetThreadResume(taos_qset param);
int        taosAddIntoQset(taos_qset, taos_queue, void *ahandle);
void       taosRemoveFromQset(taos_qset, taos_queue);
int        taosGetQueueNumber(taos_qset);

int        taosReadQitemFromQset(taos_qset, void **pItem, void **ahandle, FProcessItem *);
int        taosReadAllQitemsFromQset(taos_qset, taos_qall, void **ahandle, FProcessItems *);

int        taosGetQueueItemsNumber(taos_queue param);
int        taosGetQsetItemsNumber(taos_qset param);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_QUEUE_H*/


