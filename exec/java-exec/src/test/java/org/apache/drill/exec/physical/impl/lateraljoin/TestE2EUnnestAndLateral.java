/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.lateraljoin;

import org.apache.drill.test.BaseTestQuery;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Paths;

import static junit.framework.TestCase.fail;

@Ignore
public class TestE2EUnnestAndLateral extends BaseTestQuery {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestE2EUnnestAndLateral.class);

  private static final String regularTestFile_1 = "cust_order_10_1.json";
  private static final String regularTestFile_2 = "cust_order_10_2.json";
  private static final String schemaChangeFile_1 = "cust_order_10_2_stringNationKey.json";
  private static final String schemaChangeFile_2 = "cust_order_10_2_stringOrderShipPriority.json";

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_1));
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_2));
  }

  @Test
  public void testSubQuerySql() throws Exception {
    String Sql = "select t2.os.o_shop1 from (select t.orders as os from cp.`lateraljoin/nested-customer.parquet` t) t2";
    test(Sql);
  }

  @Test
  public void testLateralWithLimit() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT O.o_id, O.o_amount FROM UNNEST(customer.orders) O LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testLateralWithFilter() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT O.o_id, O.o_amount FROM UNNEST(customer.orders) O WHERE O.o_amount > 10) orders";
    test(Sql);
  }

  @Test
  public void testLateralWithFilterAndLimit() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT O.o_id, O.o_amount FROM UNNEST(customer.orders) O WHERE O.o_amount > 10 LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testOuterApplyWithFilterAndLimit() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer OUTER APPLY " +
      "(SELECT O.o_id, O.o_amount FROM UNNEST(customer.orders) O WHERE O.o_amount > 10 LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testLeftLateralWithFilterAndLimit() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer LEFT JOIN LATERAL " +
      "(SELECT O.o_id, O.o_amount FROM UNNEST(customer.orders) O WHERE O.o_amount > 10 LIMIT 1) orders ON TRUE";
    test(Sql);
  }

  @Ignore("Nested repeated type columns doesn't work here")
  @Test
  public void testNestedUnnest() throws Exception {
    String Sql = "EXPLAIN PLAN FOR SELECT customer.c_name, customer.c_address, U1.order_id, U1.order_amt," +
      " U1.itemName, U1.itemNum" + " FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL" +
      " (SELECT O.o_id AS order_id, O.o_amount AS order_amt, U2.item_name AS itemName, U2.item_num AS itemNum" +
      " FROM UNNEST(customer.orders) O, LATERAL" +
      " (SELECT I.i_name AS item_name, I.i_number AS item_num FROM UNNEST(O.items) AS I) AS U2) AS U1";
    test(Sql);
  }

  @Test
  public void testMultipleBatchesLateralQuery() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O) orders";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateralWithLimit() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O LIMIT 10) orders";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateralWithLimitFilter() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O WHERE O.o_totalprice > 100000 LIMIT 2) " +
      "orders";
    test(sql);
  }

  @Test
  public void testSchemaChangeOnNonUnnestColumn() throws Exception {

    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_1));

      String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " + "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " + "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O) " + "orders";
      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_1));
    }
  }

  @Test
  public void testSchemaChangeOnUnnestColumn() throws Exception {
    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));

      String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
        "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
        "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O) orders";
      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));
    }
  }

  /*
  @Test
  public void testSchemaChangeFlatten() throws Exception {
    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));

      //String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " + "FROM
      // dfs.`lateraljoin/multipleFiles` customer, LATERAL " + "(SELECT O.o_orderkey, O.o_totalprice FROM UNNEST(customer.c_orders) O) " + "orders";
      String sql = "SELECT customer.c_name, customer.c_address, FLATTEN(customer.c_orders)" +
        " FROM dfs.`lateraljoin/multipleFiles` customer";
      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));
    }
  }
  */

  /*
  @Test
  public void testSchemaChangeJson() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "TestJsonFiles"));
    String sql = "SELECT * FROM dfs.`lateraljoin/TestJsonFiles`";
    test(sql);
  }
  */
}
