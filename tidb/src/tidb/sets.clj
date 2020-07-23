(ns tidb.sets
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [knossos.op :as op]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]))

(defrecord SetClient [conn tbl-created?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! tbl-created? false true)
      (c/with-conn-failure-retry conn
        (c/execute! conn ["create table if not exists sets
                          (id     int not null primary key auto_increment,
                          value  bigint not null)"])
        (c/when-tiflash-replicas [n test]
          (info "Set tiflash replicas of sets to" n)
          (c/execute! conn [(str "alter table sets set tiflash replica " n)])
          (Thread/sleep 10000)))))

  (invoke! [this test op]
    (c/with-error-handling op
      (c/with-txn-aborts op
        (case (:f op)
          :add  (do (c/execute! conn [(str "set @@session.tidb_isolation_read_engines='tikv'")])
                    (c/insert! conn :sets (select-keys op [:value]))
                    (assoc op :type :ok))

          :read (->> (do (c/execute! conn [(str "set @@session.tidb_isolation_read_engines='tiflash'")])
                         (c/query conn ["select * from sets"]))
                     (mapv :value)
                     (assoc op :type :ok, :value))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

; This variant does compare-and-set on a single text value to reveal lost
; updates.
(defrecord CasSetClient [conn tbl-created?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! tbl-created? false true)
      (c/with-conn-failure-retry conn
        (c/execute! conn ["create table if not exists sets
                          (id     int not null primary key,
                          value   text)"])
        (c/when-tiflash-replicas [n test]
          (info "Set tiflash replicas of sets to" n)
          (c/execute! conn [(str "alter table sets set tiflash replica " n)])
          (Thread/sleep 10000)))))

  (invoke! [this test op]
    (c/with-txn op [c conn {:isolation (get test :isolation :repeatable-read)}]
      (case (:f op)
        :add  (let [e (:value op)]
                (if-let [v (-> (do (c/execute! c [(str "set @@session.tidb_isolation_read_engines='tikv'")])
                                 (c/query c [(str "select (value) from sets"
                                                   " where id = 0 "
                                                   (:read-lock test))]))
                               first
                               :value)]
                  (c/execute! c ["update sets set value = ? where id = 0"
                                    (str v "," e)])
                  (c/insert! c :sets {:id 0, :value (str e)}))
                (assoc op :type :ok))

        :read (let [v (-> (do (c/execute! c [(str "set @@session.tidb_isolation_read_engines='tiflash'")])
                            (c/query c ["select (value) from sets where id = 0"]))
                          first
                          :value)
                    v (when v
                        (->> (str/split v #",")
                             (map #(Long/parseLong %))))]
                (assoc op :type :ok, :value v)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       (gen/seq)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (SetClient. nil (atom false))
     :generator (->> (gen/reserve (/ c 2) (adds) (reads))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))

(defn cas-workload
  [opts]
  (assoc (workload opts) :client (CasSetClient. nil (atom false))))
