(ns fluree.ledger.core
  (:require [fluree.ledger.flake :as flake]
            [fluree.ledger.constants :as constants]
            [fluree.crypto :as crypto]
            [abracad.avro :as avro]
            [clojure.java.io :as io]
            [cheshire.core :as cjson])
  (:import (java.io ByteArrayOutputStream)
           (java.util UUID)
           (java.net URI)))

(def avro-bigint
  (avro/parse-schema
    {:type   :record
     :name   "BigInteger"
     :fields [{:name "val", :type :string}]}))


(defn ->BigInteger
  [val]
  (bigint val))

(def avro-bigdec
  (avro/parse-schema
    {:type   :record
     :name   "BigDecimal"
     :fields [{:name "val", :type :string}]}))

(defn ->BigDecimal
  [val]
  (bigdec val))

(def avro-uri
  (avro/parse-schema
    {:type   :record
     :name   "URI"
     :fields [{:name "val", :type :string}]}))

(defn ->URI
  [val]
  (URI. val))

(def avro-uuid
  (avro/parse-schema
    {:type   :record
     :name   "UUID"
     :fields [{:name "val", :type :string}]}))

(defn ->UUID
  [val]
  (UUID/fromString val))

(def avro-Flake
  (avro/parse-schema
    avro-bigint avro-bigdec avro-uri avro-uuid
    {:type           :record
     :name           'fluree.Flake
     :abracad.reader "vector"
     :fields         [{:name "s", :type :long}
                      {:name "p", :type :long}
                      {:name "o", :type [:long :int :string :boolean :float :double "BigInteger" "BigDecimal" "URI" "UUID"]}
                      {:name "t", :type :long}
                      {:name "op", :type :boolean}
                      {:name "m", :type [:string :null]}]}))

(def FdbBlock-schema
  (avro/parse-schema
    avro-Flake
    {:type      :record
     :name      "FdbBlock"
     :namespace "fluree"
     :fields    [{:name "block", :type :long}
                 {:name "t", :type :long}
                 {:name "flakes", :type {:type  :array
                                         :items "fluree.Flake"}}]}))



(def ^:const bindings {'fluree/Flake #'flake/->Flake
                       'BigInteger   #'->BigInteger
                       'BigDecimal   #'->BigDecimal
                       'URI          #'->URI
                       'UUID         #'->UUID})

(defn read-file
  "Returns nil if file does not exist."
  [path]
  (try
    (with-open [xin  (io/input-stream path)
                xout (ByteArrayOutputStream.)]
      (io/copy xin xout)
      (.toByteArray xout))
    (catch java.io.FileNotFoundException e
      nil)
    (catch Exception e (throw e))))


(defn deserialize-block
  [file-path]
  (binding [avro/*avro-readers* bindings]
    (avro/decode FdbBlock-schema
                 (read-file (io/file file-path)))))


(defn zero-pad
  "Zero pads x"
  [x pad]
  (loop [s (str x)]
    (if (< (.length s) pad)
      (recur (str "0" s))
      s)))


(defn ledger-block-path
  [network ledger-id block]
  (str network "/" ledger-id "/block/" (zero-pad block 15) ".fdbd"))


(defn read-block
  [data-dir network lid int]
  (let [block-path (str data-dir (ledger-block-path network lid int))
        block-data (deserialize-block block-path)]
    block-data))

(defn get-block
  [data-dir network lid int meta?]
  (let [block-data  (read-block data-dir network lid int)
        block-data* (if meta?
                      block-data
                      {:block  (:block block-data)
                       :t      (:t block-data)
                       :flakes (filter #(< 0 (.-s %)) (:flakes block-data))})]
    block-data*))

(defn get-auth
  [data-dir nw lid startBlock endBlock]
  (let [blocks (if endBlock (range startBlock (inc endBlock)) [startBlock])]
    (loop [[block & r] blocks
           acc {}]
      (if-not block
        acc
        (let [block-data (read-block data-dir nw lid block)
              auth       (some #(when (= (.-p %) constants/$_tx:auth)
                                  (.-o %)) (:flakes block-data))
              authority  (some #(when (= (.-p %) constants/$_tx:authority)
                                  (.-o %)) (:flakes block-data))]
          (recur r (merge acc {block {:auth      auth
                                      :authority authority}})))))))

(defn verify-blocks
  [data-dir nw lid startBlock endBlock]
  (let [blocks (if endBlock (range startBlock (inc endBlock)) [startBlock])]
    (loop [[block & r] blocks
           acc {:allValid true}]
      (if-not block
        acc
        (let [block-flakes    (-> (read-block data-dir nw lid block) :flakes)
              prevHash        (some #(when (= (.-p %) constants/$_block:prevHash) (.-o %)) block-flakes)
              hash            (some #(when (= (.-p %) constants/$_block:hash) (.-o %)) block-flakes)
              sig             (some #(when (= (.-p %) constants/$_tx:sig) (.-o %)) block-flakes)
              cmd             (some #(when (= (.-p %) constants/$_tx:tx) (.-o %)) block-flakes)
              prevHashMatch?  (if-let [prevBlockHash (-> (get acc (dec block)) :hash)]
                                (= prevHash prevBlockHash) nil)
              flakes-to-hash  (->> (filter #(not (#{constants/$_block:hash constants/$_block:ledgers
                                                    constants/$_block:sigs} (.-p %)))
                                           block-flakes)
                                   (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
                                   (cjson/encode))
              calculated-hash (crypto/sha3-256 flakes-to-hash)
              hashValid?      (= calculated-hash hash)]
          (if (= block 1)
            (recur r (merge acc {:allValid true
                                 block     {:prevHashMatch? prevHashMatch?
                                            :hashValid?     hashValid?
                                            :hash           hash
                                            :signature      nil
                                            :signingAuth    nil }}))
            (let [auth-id      (crypto/account-id-from-message cmd sig)
                  allValid?    (and (not (false? prevHashMatch?)) hashValid?)]
              (recur r (merge acc {:allValid (and (:allValid acc) allValid?)
                                   block     {:prevHashMatch? prevHashMatch?
                                              :hashValid?     hashValid?
                                              :hash           hash
                                              :signature      sig
                                              :signingAuth    auth-id }})))))))))

(comment

  encoded

  (= (crypto/sha3-256 encoded)
     "c79a95ddbe81f9f844268f5b7112c553632b9ed0440d1a2502bec3bc6f54a8e4")


  (str "./data/group/" (ledger-block-path "fluree" "test" 1))


  (def myblock (deserialize-block "/Users/plogian/fluree/fluree.db.transactor/data/ledger/test/one/block/000000000000005.fdbd"))

  (keys myblock)

  (def myflakes (:flakes myblock))

  (count myflakes)

  (map #(.-p %) myflakes)

  (def flakes-to-hash (->> (filter #(not (#{constants/$_block:hash constants/$_block:ledgers
                                            constants/$_block:sigs} (.-p %)))
                                   myflakes)
                           (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
                           (cjson/encode)))

  (def sig (some #(when (= (.-p %) constants/$_tx:sig) (.-o %)) myflakes))
  (def cmd (some #(when (= (.-p %) constants/$_tx:tx) (.-o %)) myflakes))

  (def auth (some #(when (= (.-p %) constants/$_tx:auth) (.-o %)) myflakes))
  auth

  flakes-to-hash

  (def myhash (some #(when (= (.-p %) constants/$_block:hash) (.-o %)) myflakes))
  myhash

  (crypto/sha3-256 flakes-to-hash)



  (verify-blocks "/Users/plogian/fluree/fluree.db.transactor/data/ledger/" "test" "one" 1 5)


  (.-s (first (-> myblock :flakes)))

  )





