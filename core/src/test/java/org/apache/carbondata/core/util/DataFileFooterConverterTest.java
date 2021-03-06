/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.util;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.ColumnarFormatVersion;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.*;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileHolderImpl;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.format.*;
import org.apache.carbondata.format.ColumnSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.util.DataFileFooterConverter.*;

import java.io.*;
import java.util.*;

import static junit.framework.TestCase.*;

public class DataFileFooterConverterTest {

  @Test public void testGetIndexInfo() throws Exception {
    DataFileFooterConverter dataFileFooterConverter = new DataFileFooterConverter();
    final ThriftReader thriftReader = new ThriftReader("file");
    List<Encoding> encoders = new ArrayList<>();
    encoders.add(Encoding.INVERTED_INDEX);
    encoders.add(Encoding.BIT_PACKED);
    encoders.add(Encoding.DELTA);
    encoders.add(Encoding.DICTIONARY);
    encoders.add(Encoding.DIRECT_DICTIONARY);
    encoders.add(Encoding.RLE);

    ColumnSchema columnSchema = new ColumnSchema(DataType.INT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema1 =
        new ColumnSchema(DataType.ARRAY, "column", "3", true, encoders, true);
    ColumnSchema columnSchema2 =
        new ColumnSchema(DataType.DECIMAL, "column", "3", true, encoders, true);
    ColumnSchema columnSchema3 =
        new ColumnSchema(DataType.DOUBLE, "column", "3", true, encoders, true);
    ColumnSchema columnSchema4 =
        new ColumnSchema(DataType.LONG, "column", "3", true, encoders, true);
    ColumnSchema columnSchema5 =
        new ColumnSchema(DataType.SHORT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema6 =
        new ColumnSchema(DataType.STRUCT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema7 =
        new ColumnSchema(DataType.STRING, "column", "3", true, encoders, true);

    final List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    columnSchemas.add(columnSchema1);
    columnSchemas.add(columnSchema2);
    columnSchemas.add(columnSchema3);
    columnSchemas.add(columnSchema4);
    columnSchemas.add(columnSchema5);
    columnSchemas.add(columnSchema6);
    columnSchemas.add(columnSchema7);

    final BlockIndex blockIndex = new BlockIndex();
    blockIndex.setBlock_index(new org.apache.carbondata.format.BlockletIndex());
    org.apache.carbondata.format.BlockletIndex blockletIndex1 =
        new org.apache.carbondata.format.BlockletIndex();
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStart_key("1".getBytes());
    blockletBTreeIndex.setEnd_key("3".getBytes());
    blockletIndex1.setB_tree_index(blockletBTreeIndex);
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.setMax_values(Arrays.asList(ByteBuffer.allocate(1).put((byte) 2)));
    blockletMinMaxIndex.setMin_values(Arrays.asList(ByteBuffer.allocate(1).put((byte) 1)));
    blockletIndex1.setMin_max_index(blockletMinMaxIndex);
    blockIndex.setBlock_index(blockletIndex1);
    List<Integer> column_cardinalities = new ArrayList<>();
    column_cardinalities.add(new Integer("1"));
    final org.apache.carbondata.format.SegmentInfo segmentInfo1 =
        new org.apache.carbondata.format.SegmentInfo(3, column_cardinalities);
    new MockUp<CarbonIndexFileReader>() {
      boolean mockedHasNextStatus = true;

      @SuppressWarnings("unused") @Mock public boolean hasNext() throws IOException {
        boolean temp = mockedHasNextStatus;
        mockedHasNextStatus = false;
        return temp;
      }

      @SuppressWarnings("unused") @Mock public void openThriftReader(String filePath)
          throws IOException {
        thriftReader.open();
      }

      @SuppressWarnings("unused") @Mock public IndexHeader readIndexHeader() throws IOException {
        return new IndexHeader(1, columnSchemas, segmentInfo1);
      }

      @SuppressWarnings("unused") @Mock public BlockIndex readBlockIndexInfo() throws IOException {
        return blockIndex;
      }

      @SuppressWarnings("unused") @Mock public void closeThriftReader() {
        thriftReader.close();
      }
    };

    new MockUp<IndexHeader>() {
      @SuppressWarnings("unused") @Mock public List<ColumnSchema> getTable_columns() {
        return columnSchemas;
      }
    };
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("1".getBytes());
    final DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock
      public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
          int bufferSize) {
        return dataInputStream;
      }
    };
    String[] arr = { "a", "b", "c" };
    TableBlockInfo tableBlockInfo = new TableBlockInfo("/file.carbondata", 3, "id", arr, 3, ColumnarFormatVersion.V1);
    tableBlockInfo.getBlockletInfos().setNoOfBlockLets(3);
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    tableBlockInfoList.add(tableBlockInfo);
    List<DataFileFooter> dataFileFooterList =
        dataFileFooterConverter.getIndexInfo("indexfile", tableBlockInfoList);
    byte[] exp = dataFileFooterList.get(0).getBlockletIndex().getBtreeIndex().getStartKey();
    byte[] res = "1".getBytes();
    for (int i = 0; i < exp.length; i++) {
      assertEquals(exp[i], res[i]);
    }

  }

  @Test public void testReadDataFileFooter() throws Exception {
    DataFileFooterConverter dataFileFooterConverter = new DataFileFooterConverter();
    DataFileFooter dataFileFooter = new DataFileFooter();
    List<Integer> column_cardinalities = new ArrayList<>();
    column_cardinalities.add(new Integer("1"));
    column_cardinalities.add(new Integer("2"));
    column_cardinalities.add(new Integer("3"));
    org.apache.carbondata.format.SegmentInfo segmentInfo1 =
        new org.apache.carbondata.format.SegmentInfo(3, column_cardinalities);
    List<Encoding> encoders = new ArrayList<>();
    encoders.add(Encoding.INVERTED_INDEX);
    encoders.add(Encoding.BIT_PACKED);
    encoders.add(Encoding.DELTA);
    encoders.add(Encoding.DICTIONARY);
    encoders.add(Encoding.DIRECT_DICTIONARY);
    encoders.add(Encoding.RLE);
    ColumnSchema columnSchema = new ColumnSchema(DataType.INT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema1 =
        new ColumnSchema(DataType.ARRAY, "column", "3", true, encoders, true);
    ColumnSchema columnSchema2 =
        new ColumnSchema(DataType.DECIMAL, "column", "3", true, encoders, true);
    ColumnSchema columnSchema3 =
        new ColumnSchema(DataType.DOUBLE, "column", "3", true, encoders, true);
    ColumnSchema columnSchema4 =
        new ColumnSchema(DataType.LONG, "column", "3", true, encoders, true);
    ColumnSchema columnSchema5 =
        new ColumnSchema(DataType.SHORT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema6 =
        new ColumnSchema(DataType.STRUCT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema7 =
        new ColumnSchema(DataType.STRING, "column", "3", true, encoders, true);
    final List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    columnSchemas.add(columnSchema1);
    columnSchemas.add(columnSchema2);
    columnSchemas.add(columnSchema3);
    columnSchemas.add(columnSchema4);
    columnSchemas.add(columnSchema5);
    columnSchemas.add(columnSchema6);
    columnSchemas.add(columnSchema7);
    org.apache.carbondata.format.BlockletIndex blockletIndex1 =
        new org.apache.carbondata.format.BlockletIndex();
    List<org.apache.carbondata.format.BlockletIndex> blockletIndexArrayList = new ArrayList<>();
    blockletIndexArrayList.add(blockletIndex1);
    org.apache.carbondata.format.BlockletInfo blockletInfo =
        new org.apache.carbondata.format.BlockletInfo();
    List<org.apache.carbondata.format.BlockletInfo> blockletInfoArrayList = new ArrayList<>();
    blockletInfoArrayList.add(blockletInfo);
    final FileFooter fileFooter = 
        new FileFooter(1, 3, columnSchemas, segmentInfo1, blockletIndexArrayList);
    fileFooter.setBlocklet_info_list(blockletInfoArrayList);
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStart_key("1".getBytes());
    blockletBTreeIndex.setEnd_key("3".getBytes());
    blockletIndex1.setB_tree_index(blockletBTreeIndex);
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.setMax_values(Arrays.asList(ByteBuffer.allocate(1).put((byte) 2)));
    blockletMinMaxIndex.setMin_values(Arrays.asList(ByteBuffer.allocate(1).put((byte) 1)));
    blockletIndex1.setMin_max_index(blockletMinMaxIndex);
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path) {
        return FileFactory.FileType.LOCAL;
      }

      @SuppressWarnings("unused") @Mock
      public FileHolder getFileHolder(FileFactory.FileType fileType) {
        return new FileHolderImpl();
      }

    };

    new MockUp<FileHolderImpl>() {
      @SuppressWarnings("unused") @Mock public long readLong(String filePath, long offset) {
        return 1;
      }
    };

    new MockUp<CarbonFooterReader>() {
      @SuppressWarnings("unused") @Mock public FileFooter readFooter() throws IOException {
        return fileFooter;
      }
    };
    SegmentInfo segmentInfo = new SegmentInfo();
    int[] arr = { 1, 2, 3 };
    segmentInfo.setColumnCardinality(arr);
    segmentInfo.setNumberOfColumns(segmentInfo1.getNum_cols());
    dataFileFooter.setNumberOfRows(3);
    dataFileFooter.setSegmentInfo(segmentInfo);
    TableBlockInfo info = new TableBlockInfo("/file.carbondata", 1, "0", new String[0], 1, ColumnarFormatVersion.V1);
    DataFileFooter result = dataFileFooterConverter.readDataFileFooter(info);
    assertEquals(result.getNumberOfRows(), 3);
  }

}
