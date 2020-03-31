#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "heap_file.h"


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

HP_ErrorCode HP_Init() { //Didn't add anything in HP_Init() since it wasn't necessary
  
  return HP_OK;
}

HP_ErrorCode HP_CreateFile(const char *filename) {

  BF_Block *hp_block;
  BF_Block_Init(&hp_block); //Allocating a temp block

  int fd;
  char* init_data;
  CALL_BF(BF_CreateFile(filename)); 
  CALL_BF(BF_OpenFile(filename, &fd)); //Creating and opening the heap file in order to place the metadata we need

  CALL_BF(BF_AllocateBlock(fd, hp_block)); //Allocating the first block for the heap file
  init_data = BF_Block_GetData(hp_block); //Getting its data
  char message[] = "This is a Heap File."; //I chose to place a string as the special information for the first block
  memcpy(init_data, message, strlen(message)+1); //Using memcpy to change the block's data 
  BF_Block_SetDirty(hp_block); //Using SetDirty since we changed the data
  CALL_BF(BF_UnpinBlock(hp_block)); //Taking care of the bytes allocated from BF_AllocateBlock()


  BF_Block_Destroy(&hp_block); //Taking care of the bytes allocated from BF_Block_Init()
  CALL_BF(BF_CloseFile(fd));
  
  
  return HP_OK;
}

HP_ErrorCode HP_OpenFile(const char *fileName, int *fileDesc){
  
  
  CALL_BF(BF_OpenFile(fileName, fileDesc));

  char* data_check;
  BF_Block *hp_block;
  BF_Block_Init(&hp_block);//Allocating a temp block

  CALL_BF(BF_GetBlock(*fileDesc, 0, hp_block)); //Getting the first block
  data_check = BF_Block_GetData(hp_block); //Getting its data
  
  if (strcmp(data_check,"This is a Heap File.")==0){ //Making sure the first block has the information we need 
    printf("All good, it is a heap file! \n");
  }


  CALL_BF(BF_UnpinBlock(hp_block)); //Taking care of the bytes allocated from BF_GetBlock()
  BF_Block_Destroy(&hp_block); //Taking care of the bytes allocated from BF_Block_Init()


  return HP_OK;
}

HP_ErrorCode HP_CloseFile(int fileDesc) {
  

  CALL_BF(BF_CloseFile(fileDesc)); //Closing the heap file
  
  return HP_OK;
}

HP_ErrorCode HP_InsertEntry(int fileDesc, Record record) {
  
  
  static int slot = 0; //The slot in the block where the record will be placed
  static int blk_counter = 1; //Counting each block, we start from 1 since block number zero is reserved for the special information
  static int bytes = 0; //Counting how many bytes we have reserved in each block
  int size = BF_BLOCK_SIZE; //Getting block's size

  char* data;
  BF_Block *hp_block;
  BF_Block_Init(&hp_block);//Allocating a temp block

  if(slot == 0){ //If slot is 0 we need to allocate a new block since the first entry is placed at slot number 0.
    CALL_BF(BF_AllocateBlock(fileDesc, hp_block)); 
  }else{ //Otherwise we get an already existing block to insert entries 1 from 7
    CALL_BF(BF_GetBlock(fileDesc, blk_counter , hp_block));
  }
  
  data = BF_Block_GetData(hp_block);
  
  int offset = slot * sizeof(Record); //Offset is where the next entry will be placed, inside the block. 
  memcpy(data+offset, &record, sizeof(Record)); //Memcpy is used to change the block's data
  bytes = bytes + sizeof(Record); //We reserved 60 bytes here, so we need to increment this variable in order to know when to allocate a new block
  BF_Block_SetDirty(hp_block); //Using SetDirty since we changed the data 
  CALL_BF(BF_UnpinBlock(hp_block));//Taking care of the bytes allocated from BF_AllocateBlock() or BF_GetBlock()

  slot++; //We placed an entry at this slot, so we increment it for the next one
  if (bytes + sizeof(Record) > BF_BLOCK_SIZE ){ //If the bytes ever exceed the block's size this means no more records can be placed so we allocate a new one.
    slot = 0;
    blk_counter++;
    bytes = 0;
  }

  BF_Block_Destroy(&hp_block); //Taking care of the bytes allocated from BF_Block_Init()
  
  return HP_OK;
}



HP_ErrorCode HP_PrintAllEntries(int fileDesc, char *attrName, void* value) {
  
  
  int slot = 0; //Same values and logic used like in HP_InsertEntry()
  int blk_counter = 1;
  int bytes = 0;
  int size = BF_BLOCK_SIZE;

  int blocks_num;
  CALL_BF(BF_GetBlockCounter(fileDesc, &blocks_num)); //Getting how many blocks are in the heap file

  BF_Block *hp_block;
  BF_Block_Init(&hp_block);

  while(blk_counter<blocks_num){ //While there are still blocks left to check
    char* data;
  
    CALL_BF(BF_GetBlock(fileDesc, blk_counter, hp_block));
    data = BF_Block_GetData(hp_block);
    bytes = bytes + sizeof(Record);

    int offset = slot * sizeof(Record);
    char* ptr_move = data+offset;
    Record* record = (Record*)ptr_move; //Using pointer arithmetics and casting to get the record we need
    if (value == NULL){ //If value is NULL we print everything
      printf("%d %s %s %s \n", record->id, record->name, record->surname, record->city );
    }else{ //Otherwise we check the field name and the corresponding value
      if(strcmp(attrName,"id")==0){
        if(record->id==(long int)value){
          printf("%d %s %s %s \n", record->id, record->name, record->surname, record->city );
        }
      }else if(strcmp(attrName,"name")==0){
        if( strcmp(record->name,(char*)value)==0 ){
          printf("%d %s %s %s \n", record->id, record->name, record->surname, record->city );
        }
      }else if(strcmp(attrName,"surname")==0){
        if( strcmp(record->surname,(char*)value)==0 ){
          printf("%d %s %s %s \n", record->id, record->name, record->surname, record->city );
        }
      }else if(strcmp(attrName,"city")==0){
        if( strcmp(record->city,(char*)value)==0 ){
          printf("%d %s %s %s \n", record->id, record->name, record->surname, record->city );
        }
      }
    }
    
    CALL_BF(BF_UnpinBlock(hp_block));
  
    slot++;
    if (bytes + sizeof(Record) > BF_BLOCK_SIZE ){ 
      slot = 0;
      blk_counter++;
      bytes = 0;
    }
  }


  BF_Block_Destroy(&hp_block);
   

  return HP_OK;
}



HP_ErrorCode HP_GetEntry(int fileDesc, int rowId, Record *record) {
  
  int recs_per_block = BF_BLOCK_SIZE/sizeof(Record); //Getting how many records are in each block
  rowId--; //rowId is decremented so div and mod can work properly.
  int get_block = rowId/recs_per_block; //Using div we get the block where the record is placed
  get_block++; //Incremented since the first one has the special information
  int get_slot = rowId%recs_per_block; //Getting its slot in the record using mod

  BF_Block *hp_block; 
  BF_Block_Init(&hp_block);
  CALL_BF(BF_GetBlock(fileDesc, get_block , hp_block));
  char* data = BF_Block_GetData(hp_block);

  char* ptr_move = data+(get_slot * sizeof(Record));
  record = (Record*)ptr_move; //Using pointer arithmetics and casting to get the record we need and place it in the record argument  
  CALL_BF(BF_UnpinBlock(hp_block)); 
  BF_Block_Destroy(&hp_block);


  return HP_OK;
}
