(ns tidb.txn
  "Client for transactional workloads."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [tidb.sql :as c :refer :all]))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test table-count [f k v]]
  (let [table (table-for table-count k)]
    [f k (case f
           :r (-> conn
                  ((fn [c] (do (c/execute! c [(str "set @@session.tidb_isolation_read_engines='tiflash'")])
                         (c/query c [(str "select val from " table " where "
                                   (if (or (:use-index test)
                                         (:predicate-read test))
                                   "sk"
                                   "id")
                                   " = ? "
                                   (:read-lock test))
                              k]))) ,,,)
                  first
                  :val)

           :w (do (c/execute! conn [(str "set @@session.tidb_isolation_read_engines='tikv'")])
                (c/execute! conn [(str "insert into " table
                                         " (id, sk, val) values (?, ?, ?)"
                                         " on duplicate key update val = ?")
                                    k k v v])
                v)

           :append
           (let [r (do (c/execute! conn [(str "set @@session.tidb_isolation_read_engines='tikv'")])
                     (c/execute!
                                 conn
                                 [(str "insert into " table
                           " (id, sk, val) values (?, ?, ?)"
                           " on duplicate key update val = CONCAT(val, ',', ?)")
                      k k (str v) (str v)]))]
             v))]))

(defrecord Client [conn val-type table-count tbl-created?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (locking Client
      (when (compare-and-set! tbl-created? false true)
        (dotimes [i table-count]
          (c/with-conn-failure-retry conn
            (c/execute! conn [(str "create table if not exists " (table-name i)
                                   " (id  int not null primary key,
                                   sk  int not null,
                                   val " val-type ")")])
            (c/when-tiflash-replicas [n test]
              (info "Set tiflash replicas of" (table-name i) "to" n)
              (c/execute! conn [(str "alter table " (table-name i) " set tiflash replica " n)])
              (when (= i (- table-count 1)) (Thread/sleep 10000)))
            (when (:use-index test)
              (c/create-index! conn [(str "create index " (table-name i) "_sk_val"
                                          " on " (table-name i) " (sk, val)")])))))))

  (invoke! [this test op]
    (let [txn      (:value op)
          use-txn? (< 1 (count txn))]
          ;use-txn? false]
          (if use-txn?
            (c/with-txn op [c conn {:isolation (get test :isolation :repeatable-read)}]
              (assoc op :type :ok, :value
                     (mapv (partial mop! c test table-count) txn)))
            (c/with-error-handling op
              (assoc op :type :ok, :value
                     (mapv (partial mop! conn test table-count) txn))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn client
  "Constructs a transactional client. Opts are:

    :val-type     An SQL type string, like \"int\", for the :val field schema.
    :table-count  How many tables to stripe records over."
  [opts]
  (Client. nil
           (:val-type opts "int")
           (:table-count opts 7)
           (atom false)))
