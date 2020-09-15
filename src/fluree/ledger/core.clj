(ns fluree.ledger.core
  (:require [fluree.ledger.flake :as flake]
            [fluree.ledger.constants :as constants]
            [com.fluree/crypto :as crypto]
            [abracad.avro :as avro]
            [clojure.java.io :as io]
            [cheshire.core :as cjson]
            [clojure.string :as str]
            [fluree.util :as util])
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


(defn ledger-block-path
  [network ledger-id block]
  (str network "/" ledger-id "/block/" (format "%015d" block) ".fdbd"))

(defn ledger-block-path*
  [ledger block]
  (str ledger "/block/" (format "%015d" block) ".fdbd"))

(defn read-block
  [data-dir ledger int]
  (let [block-path (str data-dir (ledger-block-path* ledger int))
        file-bytes (read-file (io/file block-path))
        block-data (binding [avro/*avro-readers* bindings]
                     (avro/decode FdbBlock-schema file-bytes))]
    block-data))

(defn max-block-from-disk
  "Returns the highest block found on the disk."
  [data-dir ledger]
  (let [block-path (str data-dir ledger "/block/")
        file-names (mapv str (filter #(.isFile %) (file-seq (clojure.java.io/file block-path))))
        blocks     (map #(-> (re-find #"(\d+)\.fdbd$" %) second (Integer/parseInt)) file-names)]
    (when (empty? blocks)
      (throw (ex-info (str "Unable to find any block files in directory: " block-path)
                      {:status 500
                       :error  :db/invalid-ledger})))
    (apply max blocks)))

(defn- get-block-range
  "Given a start-block and end-block which are both option, returns a valid block range
  using data on disk if necessary."
  [data-dir ledger start-block end-block]
  (cond
    (and start-block end-block)
    (range start-block (inc end-block))

    start-block
    (range start-block (inc (max-block-from-disk data-dir ledger)))

    :else
    (range 1 (inc (max-block-from-disk data-dir ledger)))))

(defn read-block+hash
  "Like read-block, but also returns sha-256 hash of file bytes"
  [data-dir ledger int]
  (let [block-path (str data-dir (ledger-block-path* ledger int))
        file-bytes (read-file (io/file block-path))
        block-data (binding [avro/*avro-readers* bindings]
                     (avro/decode FdbBlock-schema file-bytes))]
    [block-data (crypto/sha2-256 file-bytes :hex :bytes)]))

(defn get-block
  [data-dir ledger block meta? opts]
  (let [block-data  (read-block data-dir ledger block)
        block-data* (if meta?
                      block-data
                      {:block  (:block block-data)
                       :t      (:t block-data)
                       :flakes (filter #(< 0 (.-s %)) (:flakes block-data))})]
    block-data*))

(defn get-auth
  [data-dir ledger startBlock endBlock opts]
  (let [blocks (get-block-range data-dir ledger startBlock endBlock)]
    (loop [[block & r] blocks
           acc (sorted-map)]
      (if-not block
        acc
        (let [block-data (read-block data-dir ledger block)
              auth       (some #(when (= (.-p %) constants/$_tx:auth)
                                  (.-o %)) (:flakes block-data))
              authority  (some #(when (= (.-p %) constants/$_tx:authority)
                                  (.-o %)) (:flakes block-data))]
          (recur r (merge acc {block {:auth      auth
                                      :authority authority}})))))))

(defn verify-blocks
  [data-dir ledger startBlock endBlock opts]
  (let [blocks (get-block-range data-dir ledger startBlock endBlock)]
    ;(println "blocks: " (pr-str blocks))
    (loop [[block & r] blocks
           invalid-blocks []
           acc            (sorted-map)]
      (if-not block
        [acc (not-empty invalid-blocks)]
        (let [block-flakes    (-> (read-block data-dir ledger block) :flakes)
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
            (recur r invalid-blocks (assoc acc block {:prevHashMatch? prevHashMatch?
                                                      :hashValid?     hashValid?
                                                      :hash           hash
                                                      :signingAuth    nil}))
            (let [auth-id         (crypto/account-id-from-message cmd sig)
                  valid?          (and (not (false? prevHashMatch?)) hashValid?)
                  invalid-blocks* (if valid? invalid-blocks (conj invalid-blocks block))]
              (recur r invalid-blocks* (assoc acc block {:prevHashMatch? prevHashMatch?
                                                         :hashValid?     hashValid?
                                                         :hash           hash
                                                         :signingAuth    auth-id})))))))))

(comment

  (verify-blocks "/Users/bplatz/tmp/ledgera1/ledger/"
                 "fabric/mvp4"
                 4 nil nil)

  (crypto/account-id-from-message "{\"type\":\"tx\",\"db\":\"fabric/mvp4\",\"tx\":[{\"_id\":\"user\",\"wallet\":{\"_id\":\"wallet\",\"name\":\"adminWallet\",\"balance\":10,\"earnings\":10},\"adminAuth\":\"_auth$1\",\"firstName\":\"Admin\",\"lastName\":\"Account\",\"profilePic\":\"initialData/users/5.jpg\",\"handle\":\"admin\",\"registered\":true,\"shoppingInterests\":[[\"category/descriptiveName\",\"clothing.mens\"],[\"category/descriptiveName\",\"clothing.mens.casual\"],[\"category/descriptiveName\",\"travel\"]]},{\"_id\":\"_auth$1\",\"id\":\"TfCywfruQaPvZcTWhCsnYHRASHfM3VF6PhB\",\"roles\":[\"_role$1\"]},{\"_id\":\"_role$1\",\"id\":\"Admin\",\"doc\":\"Fabric Admin Role\",\"rules\":[[\"_rule/id\",\"root\"]]}],\"nonce\":1566399776721,\"auth\":\"Tf39wv7cz29KpXDRMxSgn3KGCWXB3PrKgw6\",\"expire\":1566399806723}"
                                  "1b30440220032737c99c72dda109d6373f4f80c01358a1e1a76b023398b8b0120b5ad0349c022062d8a0f2b5bfbd43b323ac4a67dda32b7c098f0a7bc97e15be4f039617086534")
  )

(defn- range-gap
  "Determines if a set of integers are continuous, and if a gap exists
  returns first missing number. int-set order is irrelevant."
  [int-set]
  (let [sint (sort int-set)                                 ;; sort inputs
        cmp  (range (first sint) (inc (last sint)))         ;; list of continuous comparison values
        ]
    (->> (interleave sint cmp)
         (partition 2)
         (some #(when (not= (first %) (second %))
                  (second %))))))


(defn- tx-item-report
  [block tx-flakes]
  (let [t           (.-s (first tx-flakes))
        cmd         (some #(when (= (.-p %) constants/$_tx:tx) (.-o %)) tx-flakes)
        txid        (some #(when (= (.-p %) constants/$_tx:id) (.-o %)) tx-flakes)
        tx-error    (some #(when (= (.-p %) constants/$_tx:error) (.-o %)) tx-flakes)
        txid-calc   (when cmd (crypto/sha3-256 cmd))
        txid-match? (and txid (= txid txid-calc))
        warnings    (when (and (not txid-match?) (not= 1 block)) ;; genesis block doesn't have a command to calc from
                      [(str "Block " block " has an inconsistent txid for t: " t "."
                            " _tx/id recorded as: " txid " but calculated as: " txid-calc ".")])
        expire      (some-> cmd cjson/decode (get "expire"))]
    (util/without-nils
      {:t        t
       :txid     txid-calc
       :expire   expire
       :expire-t (when expire (str (java.time.Instant/ofEpochMilli expire)))
       :tx-error tx-error
       :warnings (not-empty warnings)
       :cmd      cmd})))


(defn tx-report
  "Valid opts include:
   --output-file=myfile.json - outputs relative to directory cli run in, or use absolute path"
  [data-dir ledger startBlock endBlock opts]
  (let [blocks      (get-block-range data-dir ledger startBlock endBlock)]
    (loop [[block & r] blocks
           expected-t   nil                                 ;; expected first t value of block
           all-warnings []
           reports      (sorted-map)]
      (if-not block
        [reports (not-empty all-warnings)]
        (let [[block-data checksum] (read-block+hash data-dir ledger block)
              flakes       (:flakes block-data)
              tx-map       (->> flakes                      ;; group all tx flakes by t
                                (filter #(< (.-s %) 0))     ;; all subject-ids < 0
                                (group-by #(.-s %)))
              t-list       (sort > (keys tx-map))
              block-t      (last t-list)
              block-flakes (get tx-map block-t)             ;; the last 't' value should be the block meta
              block-n      (some #(when (= (.-p %) constants/$_block:number) (.-o %)) block-flakes)
              instant      (some #(when (= (.-p %) constants/$_block:instant) (.-o %)) block-flakes)
              tx-map*      (dissoc tx-map block-t)          ;; remove block meta, should be left with only tx / commands
              tx-reports   (->> (vals tx-map*)              ;; a report for each tx returned as a map
                                (reduce (fn [acc t-flakes]
                                          (conj acc (tx-item-report block t-flakes))) []))
              warnings     (cond-> (keep :warnings tx-reports) ;; get any warnings from within individual txs
                                   ;; gap in 't' values for this block
                                   (and expected-t (not= expected-t (first t-list)))
                                   (conj (str "Block " block " has a gap in 't' values. "
                                              "Expected block to start with t: " expected-t
                                              " but instead started with: " (first t-list) "."))
                                   ;; missing 't' inside the block
                                   (range-gap t-list)
                                   (str "Block " block " is has a missing t value of: " (range-gap t-list) "."))
              report       (util/without-nils
                             {:file      (str data-dir (ledger-block-path* ledger block)) ;; output path of file we read
                              :block     block-n
                              :block-t   block-t
                              :instant   instant
                              :instant-t (str (java.time.Instant/ofEpochMilli instant))
                              :tx-count  (count tx-reports)
                              :txs       tx-reports
                              :warnings  (not-empty warnings)
                              :checksum  checksum           ;; sha-256 hash of file bytes
                              })]
          (recur r (dec block-t) (concat all-warnings warnings) (assoc reports block report)))))))


(comment

  (tx-report "/Users/bplatz/tmp/ledgera1/ledger/"
             "fabric"
             "mvp4"
             10 nil
             {:output-file "ledgera1.json"})
  (tx-report "/Users/bplatz/tmp/ledgerc1/ledger/"
             "fabric"
             "mvp4"
             1 1716
             {:output-file "ledgerc1.json"})
  (tx-report "/Users/bplatz/tmp/ledgerd1/ledger/"
             "fabric"
             "mvp4"
             1 1716
             {:output-file "ledgerd1.json"})



  (-> (slurp "ledgera1.json")
      (cjson/decode true)
      )

  (get-block "/Users/bplatz/tmp/ledgerc1/ledger/" "fdbaas/master" 771 true)

  (let [a1   (-> (slurp "ledgera1.json")
                 (cjson/decode true))
        c1   (-> (slurp "ledgerc1.json")
                 (cjson/decode true))
        d1   (-> (slurp "ledgerd1.json")
                 (cjson/decode true))
        a-tx (into #{} (->> a1 (mapcat :txs) (map :txid)))
        c-tx (into #{} (->> c1 (mapcat :txs) (map :txid)))
        d-tx (into #{} (->> d1 (mapcat :txs) (map :txid)))]
    #_(mapv (fn [a c d]
              (let [{a-block :block a-instant-t :instant-t a-txid :txid a-expire-t :expire-t} a
                    {c-block :block c-instant-t :instant-t c-txid :txid c-expire-t :expire-t} c
                    {d-block :block d-instant-t :instant-t d-txid :txid d-expire-t :expire-t} d]

                (when (not= a-txid c-txid)
                  (println "a != c @" a-block c-block a-instant-t c-instant-t a-txid c-txid a-expire-t c-expire-t))
                (when (not= a-txid d-txid)
                  (println "a != d @" a-block d-block a-instant-t d-instant-t a-txid d-txid a-expire-t d-expire-t))
                (when (not= c-txid d-txid)
                  (println "c != d @" c-block d-block c-instant-t d-instant-t c-txid d-txid c-expire-t d-expire-t)))
              )
            a1 c1 d1)
    (println "Tx only in a: " (clojure.set/difference a-tx c-tx d-tx))
    (println "Tx only in c: " (clojure.set/difference c-tx a-tx d-tx))
    (println "Tx only in d: " (clojure.set/difference d-tx a-tx c-tx))
    :done
    )

  )





