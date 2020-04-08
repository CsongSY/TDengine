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
#include "tsdbMain.h"

#define adjustMem(ptr, size, expectedSize)    \
  do {                                        \
    if ((size) < (expectedSize)) {            \
      (ptr) = realloc((void *)(ptr), (expectedSize)); \
      if ((ptr) == NULL) return -1;           \
      (size) = (expectedSize);                \
    }                                         \
  } while (0)

// Local function definitions
static int  tsdbCheckHelperCfg(SHelperCfg *pCfg);
static void tsdbInitHelperFile(SHelperFile *pHFile);
static int  tsdbInitHelperRead(SRWHelper *pHelper);
static int  tsdbInitHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperFile(SHelperFile *pHFile);
static void tsdbDestroyHelperRead(SRWHelper *pHelper);
static void tsdbDestroyHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperRead(SRWHelper *pHelper);
static void tsdbClearHelperWrite(SRWHelper *pHelper);
static bool tsdbShouldCreateNewLast(SRWHelper *pHelper);
static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite, SCompBlock *pCompBlock,
                                bool isLast, bool isSuperBlock);
static int compareKeyBlock(const void *arg1, const void *arg2);
static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);
static int nRowsLEThan(SDataCols *pDataCols, int maxKey);
static int tsdbGetRowsCanBeMergedWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);

int tsdbInitHelper(SRWHelper *pHelper, SHelperCfg *pCfg) {
  if (pHelper == NULL || pCfg == NULL || tsdbCheckHelperCfg(pCfg) < 0) return -1;

  memset((void *)pHelper, 0, sizeof(*pHelper));

  pHelper->config = *pCfg;

  tsdbInitHelperFile(&(pHelper->files));

  if (tsdbInitHelperRead(pHelper) < 0) goto _err;

  if ((TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) && tsdbInitHelperWrite(pHelper) < 0) goto _err;

  pHelper->state = TSDB_HELPER_CLEAR_STATE;

  return 0;

_err:
  tsdbDestroyHelper(pHelper);
  return -1;
}

void tsdbDestroyHelper(SRWHelper *pHelper) {
  if (pHelper == NULL) return;

  tsdbClearHelperFile(&(pHelper->files));
  tsdbDestroyHelperRead(pHelper);
  tsdbDestroyHelperWrite(pHelper);
}

void tsdbClearHelper(SRWHelper *pHelper) {
  if (pHelper == NULL) return;
  tsdbClearHelperFile(&(pHelper->files));
  tsdbClearHelperRead(pHelper);
  tsdbClearHelperWrite(pHelper);
}

int tsdbSetAndOpenHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
  ASSERT(pHelper != NULL && pGroup != NULL);

  // Clear the helper object
  tsdbClearHelper(pHelper);

  ASSERT(pHelper->state == TSDB_HELPER_CLEAR_STATE);

  // Set the files
  pHelper->files.fid = pGroup->fileId;
  pHelper->files.headF = pGroup->files[TSDB_FILE_TYPE_HEAD];
  pHelper->files.dataF = pGroup->files[TSDB_FILE_TYPE_DATA];
  pHelper->files.lastF = pGroup->files[TSDB_FILE_TYPE_LAST];
  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    char *fnameDup = strdup(pHelper->files.headF.fname);
    if (fnameDup == NULL) goto _err;
    if (fnameDup == NULL) return -1;
    char *dataDir = dirname(fnameDup);

    tsdbGetFileName(dataDir, pHelper->files.fid, ".h", pHelper->files.nHeadF.fname);
    tsdbGetFileName(dataDir, pHelper->files.fid, ".l", pHelper->files.nLastF.fname);
    free((void *)fnameDup);
  }

  // Open the files
  if (tsdbOpenFile(&(pHelper->files.headF), O_RDONLY) < 0) goto _err;
  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDWR) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDWR) < 0) goto _err;

    // Create and open .h
    if (tsdbOpenFile(&(pHelper->files.nHeadF), O_WRONLY | O_CREAT) < 0) goto _err;
    size_t tsize = TSDB_FILE_HEAD_SIZE + sizeof(SCompIdx) * pHelper->config.maxTables;
    if (tsendfile(pHelper->files.nHeadF.fd, pHelper->files.headF.fd, NULL, tsize) < tsize) goto _err;

    // Create and open .l file if should
    if (tsdbShouldCreateNewLast(pHelper)) {
      if (tsdbOpenFile(&(pHelper->files.nLastF), O_WRONLY | O_CREAT) < 0) goto _err;
      if (tsendfile(pHelper->files.nLastF.fd, pHelper->files.lastF.fd, NULL, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) goto _err;
    }
  } else {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDONLY) < 0) goto _err;
  }

  helperSetState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN);

  return tsdbLoadCompIdx(pHelper, NULL);

  _err:
  return -1;
}

int tsdbCloseHelperFile(SRWHelper *pHelper, bool hasError) {
  if (pHelper->files.headF.fd > 0) {
    close(pHelper->files.headF.fd);
    pHelper->files.headF.fd = -1;
  }
  if (pHelper->files.dataF.fd > 0) {
    close(pHelper->files.dataF.fd);
    pHelper->files.dataF.fd = -1;
  }
  if (pHelper->files.lastF.fd > 0) {
    close(pHelper->files.lastF.fd);
    pHelper->files.lastF.fd = -1;
  }
  if (pHelper->files.nHeadF.fd > 0) {
    close(pHelper->files.nHeadF.fd);
    pHelper->files.nHeadF.fd = -1;
    if (hasError) remove(pHelper->files.nHeadF.fname);
  }
  
  if (pHelper->files.nLastF.fd > 0) {
    close(pHelper->files.nLastF.fd);
    pHelper->files.nLastF.fd = -1;
    if (hasError) remove(pHelper->files.nLastF.fname);
  }
  return 0;
}

void tsdbSetHelperTable(SRWHelper *pHelper, SHelperTable *pHelperTable, STSchema *pSchema) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN));

  // Clear members and state used by previous table
  pHelper->blockIter = 0;
  pHelper->state &= (TSDB_HELPER_TABLE_SET - 1);

  pHelper->tableInfo = *pHelperTable;
  tdInitDataCols(pHelper->pDataCols[0], pSchema);
  tdInitDataCols(pHelper->pDataCols[1], pSchema);

  pHelper->compIdx = pHelper->pCompIdx[pHelper->tableInfo.tid];

  helperSetState(pHelper, TSDB_HELPER_TABLE_SET);
}

/**
 * Write part of of points from pDataCols to file
 * 
 * @return: number of points written to file successfully
 *          -1 for failure
 */
int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols) {
  ASSERT(TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER);

  SCompBlock compBlock;
  int        rowsToWrite = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);

  ASSERT(helperHasState(pHelper, TSDB_HELPER_IDX_LOAD));
  SCompIdx  curIdx = pHelper->compIdx; // old table SCompIdx for sendfile usage
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid; // for change purpose

  // Load the SCompInfo part if neccessary
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));
  if ((!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) &&
      ((pIdx->offset > 0) && (pIdx->hasLast || dataColsKeyFirst(pDataCols) <= pIdx->maxKey)) &&
      (tsdbLoadCompInfo(pHelper, NULL) < 0))
    goto _err;

  if (!pIdx->hasLast && keyFirst > pIdx->maxKey) {
    // Just need to append as a super block
    rowsToWrite = pDataCols->numOfPoints;
    SFile *pWFile = NULL;
    bool   isLast = false;

    if (rowsToWrite > pHelper->config.minRowsPerFileBlock) {
      pWFile = &(pHelper->files.dataF);
    } else {
      isLast = true;
      pWFile = (pHelper->files.nLastF.fd > 0) ? &(pHelper->files.nLastF) : &(pHelper->files.lastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pWFile, pDataCols, rowsToWrite, &compBlock, isLast, true) < 0) goto _err;

    // TODO: may need to reallocate the memory
    pHelper->pCompInfo->blocks[pHelper->blockIter++] = compBlock;

    pIdx->hasLast = compBlock.last;
    pIdx->numOfSuperBlocks++;
    pIdx->maxKey = dataColsKeyLast(pDataCols);
    // pIdx->len = ??????
  } else { // (pIdx->hasLast) OR (keyFirst <= pIdx->maxKey)
    if (keyFirst > pIdx->maxKey) {
      int blkIdx = pIdx->numOfSuperBlocks - 1;
      ASSERT(pIdx->hasLast && pHelper->pCompInfo->blocks[blkIdx].last);

      // Need to merge with the last block
      if (tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols) < 0) goto _err;
    } else {
      // Find the first block greater or equal to the block
      SCompBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)(pHelper->pCompInfo->blocks),
                                           pIdx->numOfSuperBlocks, sizeof(SCompBlock), compareKeyBlock, TD_GE);
      if (pCompBlock == NULL) {
        if (tsdbMergeDataWithBlock(pHelper, pIdx->numOfSuperBlocks-1, pDataCols) < 0) goto _err;
      } else {
        if (compareKeyBlock((void *)(&keyFirst), (void *)pCompBlock) == 0) {
          SCompBlock *pNextBlock = NULL;
          TSKEY keyLimit = (pNextBlock == NULL) ? INT_MAX : (pNextBlock->keyFirst - 1);
          rowsToWrite =
              MIN(nRowsLEThan(pDataCols, keyLimit), pHelper->config.maxRowsPerFileBlock - pCompBlock->numOfPoints);
          
          if (tsdbMergeDataWithBlock(pHelper, pCompBlock-pHelper->pCompInfo->blocks, pDataCols) < 0) goto _err;
        } else {
          // There options: 1. merge with previous block
          //                2. commit as one block
          //                3. merge with current block
          int nRows1 = INT_MAX;
          int nRows2 = nRowsLEThan(pDataCols, pCompBlock->keyFirst);
          int nRows3 = MIN(nRowsLEThan(pDataCols, (pCompBlock + 1)->keyFirst), (pHelper->config.maxRowsPerFileBlock - pCompBlock->numOfPoints));

          // TODO: find the block with max rows can merge
          if (tsdbMergeDataWithBlock(pHelper, pCompBlock, pDataCols) < 0) goto _err;
        }
      }
    }
  }

  return rowsToWrite;

  _err:
  return -1;
}

int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbWriteCompInfo(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbWriteCompIdx(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbLoadCompIdx(SRWHelper *pHelper, void *target) {
  ASSERT(pHelper->state = TSDB_HELPER_FILE_SET_AND_OPEN);

  if (!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD)) {
    // If not load from file, just load it in object
    int fd = pHelper->files.headF.fd;

    if (lseek(fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;
    if (tread(fd, (void *)(pHelper->pCompIdx), pHelper->compIdxSize) < pHelper->compIdxSize) return -1;
    // TODO: check the correctness of the part
  }
  helperSetState(pHelper, TSDB_HELPER_IDX_LOAD);

  // Copy the memory for outside usage
  if (target) memcpy(target, pHelper->pCompIdx, pHelper->compIdxSize);

  return 0;
}

int tsdbLoadCompInfo(SRWHelper *pHelper, void *target) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));

  SCompIdx curCompIdx = pHelper->compIdx;

  ASSERT(curCompIdx.offset > 0 && curCompIdx.len > 0);

  int fd = pHelper->files.headF.fd;

  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (lseek(fd, curCompIdx.offset, SEEK_SET) < 0) return -1;

    adjustMem(pHelper->pCompInfo, pHelper->compInfoSize, curCompIdx.len);
    if (tread(fd, (void *)(pHelper->pCompInfo), pHelper) < 0) return -1;
    // TODO: check the checksum

    helperSetState(pHelper, TSDB_HELPER_INFO_LOAD);
  }

  if (target) memcpy(target, (void *)(pHelper->pCompInfo), curCompIdx.len);

  return 0;
}

int tsdbLoadCompData(SRWHelper *pHelper, int blkIdx, void *target) {
  // TODO
  return 0;
}

int tsdbLoadBlockDataCols(SRWHelper *pHelper, SDataCols *pDataCols, int32_t *colIds, int numOfColIds) {
  // TODO
  return 0;
}

int tsdbLoadBlockData(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  // TODO
  return 0;
}

static int tsdbCheckHelperCfg(SHelperCfg *pCfg) {
  // TODO
  return 0;
}

static void tsdbInitHelperFile(SHelperFile *pHFile) {
  pHFile->fid = -1;
  pHFile->headF.fd = -1;
  pHFile->dataF.fd = -1;
  pHFile->lastF.fd = -1;
  pHFile->nHeadF.fd = -1;
  pHFile->nLastF.fd = -1;
}

static void tsdbClearHelperFile(SHelperFile *pHFile) {
  pHFile->fid = -1;
  if (pHFile->headF.fd > 0) {
    close(pHFile->headF.fd);
    pHFile->headF.fd = -1;
  }
  if (pHFile->dataF.fd > 0) {
    close(pHFile->dataF.fd);
    pHFile->dataF.fd = -1;
  }
  if (pHFile->lastF.fd > 0) {
    close(pHFile->lastF.fd);
    pHFile->lastF.fd = -1;
  }
  if (pHFile->nHeadF.fd > 0) {
    close(pHFile->nHeadF.fd);
    pHFile->nHeadF.fd = -1;
  }
  if (pHFile->nLastF.fd > 0) {
    close(pHFile->nLastF.fd);
    pHFile->nLastF.fd = -1;
  }

}

static int tsdbInitHelperRead(SRWHelper *pHelper) {
  SHelperCfg *pCfg = &(pHelper->config);

  pHelper->compIdxSize = pCfg->maxTables * sizeof(SCompIdx);
  if ((pHelper->pCompIdx = (SCompIdx *)malloc(pHelper->compIdxSize)) == NULL) return -1;

  return 0;
}

static void tsdbDestroyHelperRead(SRWHelper *pHelper) {
  tfree(pHelper->pCompIdx);
  pHelper->compIdxSize = 0;

  tfree(pHelper->pCompInfo);
  pHelper->compInfoSize = 0;

  tfree(pHelper->pCompData);
  pHelper->compDataSize = 0;

  tdFreeDataCols(pHelper->pDataCols[0]);
  tdFreeDataCols(pHelper->pDataCols[1]);
}

static int tsdbInitHelperWrite(SRWHelper *pHelper) {
  SHelperCfg *pCfg = &(pHelper->config);

  // pHelper->wCompIdxSize = pCfg->maxTables * sizeof(SCompIdx);
  // if ((pHelper->pWCompIdx = (SCompIdx *)malloc(pHelper->wCompIdxSize)) == NULL) return -1;

  return 0;
}

static void tsdbDestroyHelperWrite(SRWHelper *pHelper) {
  // tfree(pHelper->pWCompIdx);
  // pHelper->wCompIdxSize = 0;

  // tfree(pHelper->pWCompInfo);
  // pHelper->wCompInfoSize = 0;

  // tfree(pHelper->pWCompData);
  // pHelper->wCompDataSize = 0;
}

static void tsdbClearHelperRead(SRWHelper *pHelper) {
  // TODO
}

static void tsdbClearHelperWrite(SRWHelper *pHelper) {
  // TODO
}

static bool tsdbShouldCreateNewLast(SRWHelper *pHelper) {
  // TODO
  return 0;
}

static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite, SCompBlock *pCompBlock,
                                bool isLast, bool isSuperBlock) {
  ASSERT(rowsToWrite > 0 && rowsToWrite <= pDataCols->numOfPoints &&
         rowsToWrite <= pHelper->config.maxRowsPerFileBlock);

  SCompData *pCompData = NULL;
  int64_t offset = 0;

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) goto _err;

  pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pDataCols->numOfCols);
  if (pCompData == NULL) goto _err;

  int nColsNotAllNull = 0;
  int32_t toffset = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (0) {
      // TODO: all data are NULL
      continue;
    }

    // Compress the data here
    {}

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    pCompCol->len = TYPE_BYTES[pCompCol->type] * rowsToWrite; // TODO: change it
    pCompCol->offset = toffset;
    nColsNotAllNull++;

    toffset += pCompCol->len;
  }

  ASSERT(nColsNotAllNull > 0);

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * nColsNotAllNull;
  if (twrite(pFile->fd, (void *)pCompData, tsize) < tsize) goto _err;
  int nCompCol = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    ASSERT(nCompCol < nColsNotAllNull);

    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nCompCol;

    if (pDataCol->colId == pCompCol->colId) {
      if (twrite(pFile->fd, (void *)(pDataCol->pData), pCompCol->len) < pCompCol->len) goto _err;
      tsize += pCompCol->len;
      nCompCol++;
    }
  }

  pCompBlock->last = isLast;
  pCompBlock->offset = offset;
  pCompBlock->algorithm = 2;  // TODO
  pCompBlock->numOfPoints = rowsToWrite;
  pCompBlock->sversion = pHelper->tableInfo.sversion;
  pCompBlock->len = (int32_t)tsize;
  pCompBlock->numOfSubBlocks = isSuperBlock ? 1 : 0;
  pCompBlock->numOfCols = nColsNotAllNull;
  pCompBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pCompBlock->keyLast = dataColsKeyAt(pDataCols, rowsToWrite - 1);

  tfree(pCompData);
  return 0;

  _err:
  tfree(pCompData);
  return -1;
}

static int compareKeyBlock(const void *arg1, const void *arg2) {
  TSKEY       key = *(TSKEY *)arg1;
  SCompBlock *pBlock = (SCompBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  }

  return 0;
}

static int nRowsLEThan(SDataCols *pDataCols, int maxKey) {
  return 0;
}

static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  int        rowsWritten = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);
  SCompBlock compBlock = {0};

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  ASSERT(blkIdx < pIdx->numOfSuperBlocks);

  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pCompBlock->numOfSubBlocks >= 1);

  int rowsCanMerge = tsdbGetRowsCanBeMergedWithBlock(pHelper, blkIdx, pDataCols);
  if (rowsCanMerge < 0) goto _err;

  ASSERT(rowsCanMerge > 0);

  if (pCompBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS &&
      ((!pCompBlock->last) || (pHelper->files.nLastF.fd < 0 &&
                               pCompBlock->numOfPoints + rowsCanMerge < pHelper->config.minRowsPerFileBlock))) {

    SFile *pFile = NULL;

    if (!pCompBlock->last) {
      pFile = &(pHelper->files.dataF);
    } else {
      pFile = &(pHelper->files.lastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pFile, pDataCols, rowsCanMerge, &compBlock, pCompBlock->last, false) < 0) goto _err;

    // TODO: Add the sub-block
    if (pCompBlock->numOfSubBlocks == 1) {
      pCompBlock->numOfSubBlocks += 2;
      // pCompBlock->offset = ;
      // pCompBlock->len = ;
    } else {
      pCompBlock->numOfSubBlocks++;
    }
    pCompBlock->numOfPoints += rowsCanMerge;
    pCompBlock->keyFirst = MIN(pCompBlock->keyFirst, dataColsKeyFirst(pDataCols));
    pCompBlock->keyLast = MAX(pCompBlock->keyLast, dataColsKeyAt(pDataCols, rowsCanMerge - 1));

    // Update the Idx
    // pIdx->hasLast = ;
    // pIdx->len =;
    // pIdx->numOfSuperBlocks = ;

    rowsWritten = rowsCanMerge;
  } else {
    // Read-Merge-Write as a super block
    if (tsdbLoadBlockData(pHelper, blkIdx, NULL) < 0) goto _err;
    tdMergeDataCols(pHelper->pDataCols[0], pDataCols, rowsCanMerge);

    int isLast = 0;
    SFile *pFile = NULL;
    if (!pCompBlock->last || (pCompBlock->numOfPoints + rowsCanMerge >= pHelper->config.minRowsPerFileBlock)) {
      pFile = &(pHelper->files.dataF);
    } else {
      isLast = 1;
      if (pHelper->files.nLastF.fd > 0) {
        pFile = &(pHelper->files.nLastF);
      } else {
        pFile = &(pHelper->files.lastF);
      }
    }

    if (tsdbWriteBlockToFile(pHelper, pFile, pHelper->pDataCols[0], pCompBlock->numOfPoints + rowsCanMerge, &compBlock, isLast, true) < 0) goto _err;

    *pCompBlock = compBlock;

    pIdx->maxKey = MAX(pIdx->maxKey, compBlock.keyLast);
    // pIdx->hasLast = ;
    // pIdx->
  }

  return rowsWritten;

  _err:
  return -1;
}

static int compTSKEY(const void *key1, const void *key2) { return ((TSKEY *)key1 - (TSKEY *)key2); }

// Get the number of rows the data can be merged into the block
static int tsdbGetRowsCanBeMergedWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  int   rowsCanMerge = 0;
  TSKEY keyFirst = dataColsKeyFirst(pDataCols);

  SCompIdx *  pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(blkIdx < pIdx->numOfSuperBlocks);

  TSKEY keyMax = (blkIdx < pIdx->numOfSuperBlocks + 1) ? (pCompBlock + 1)->keyFirst - 1 : pHelper->files.maxKey;

  if (keyFirst > pCompBlock->keyLast) {
    void *ptr = taosbsearch((void *)(&keyMax), pDataCols->cols[0].pData, pDataCols->numOfPoints, sizeof(TSKEY),
                            compTSKEY, TD_LE);
    ASSERT(ptr != NULL);

    rowsCanMerge =
        MIN((TSKEY *)ptr - (TSKEY *)pDataCols->cols[0].pData, pHelper->config.minRowsPerFileBlock - pCompBlock->numOfPoints);

  } else {
    int32_t colId[1] = {0};
    if (tsdbLoadBlockDataCols(pHelper, NULL, &colId, 1) < 0) goto _err;

    int iter1 = 0;  // For pDataCols
    int iter2 = 0;  // For loaded data cols

    while (1) {
      if (iter1 >= pDataCols->numOfPoints || iter2 >= pHelper->pDataCols[0]->numOfPoints) break;
      if (pCompBlock->numOfPoints + rowsCanMerge >= pHelper->config.maxRowsPerFileBlock) break;

      TSKEY key1 = dataColsKeyAt(pDataCols, iter1);
      TSKEY key2 = dataColsKeyAt(pHelper->pDataCols[0], iter2);

      if (key1 > keyMax) break;

      if (key1 < key2) {
        iter1++;
      } else if (key1 == key2) {
        iter1++;
        iter2++;
      } else {
        iter2++;
        rowsCanMerge++;
      }
    }
  }

  return rowsCanMerge;

_err:
  return -1;
}