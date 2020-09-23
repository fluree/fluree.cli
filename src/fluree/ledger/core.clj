(ns fluree.ledger.core
  (:require [fluree.ledger.flake :as flake]
            [fluree.ledger.constants :as constants]
            [fluree.crypto :as crypto]
            [abracad.avro :as avro]
            [clojure.java.io :as io]
            [cheshire.core :as cjson]
            [clojure.string :as str]
            [fluree.util :as util]
            [clojure.set :as set])
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

(defn- get-all-directories
  "Returns list of java.io.File objects of all direct (non-recursive) directories
  located at provided path."
  [path]
  (filter #(.isDirectory %) (.listFiles (io/file path))))

(defn glob-match-ledgers
  "Given a ledger data directory and a ledger match pattern, returns list of matching ledgers."
  [data-dir ledger-match-glob]
  (let [matches-glob     (fn [s] (re-matches (util/glob->regex ledger-match-glob) s))
        ledger-from-path (fn [s] (second (re-find #".+/([^/]+/[^/]+)$" s))) ;; matches just the last */* from full path
        network-dirs     (get-all-directories data-dir)]
    (->> (mapcat get-all-directories network-dirs)          ;; get direct directories of network directories
         (map str)                                          ;; string full path names
         (map ledger-from-path)                             ;; pull out just the ledger name (i.e. some/ledger )
         (filter matches-glob)                              ;; only glob matches
         )))


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
        txid-match? (= txid txid-calc)
        warnings    (when (and (not txid-match?)
                               (not= 1 block)               ;; genesis block doesn't have a command to calc from
                               (not tx-error))              ;; we did not record a txid for errors in older versions
                      (cond-> (str "Block " block " has an inconsistent txid for t: " t "."
                                   " _tx/id recorded as: " txid " but calculated as: " txid-calc ".")
                              tx-error (str "\n   - This tx was an error: " tx-error)))
        expire      (some-> cmd cjson/decode (get "expire"))]
    (util/without-nils
      {:t        t
       :txid     txid-calc
       :expire   expire
       :expire-t (when expire (str (java.time.Instant/ofEpochMilli expire)))
       :tx-error tx-error
       :warnings warnings
       :cmd      cmd})))


(defn tx-report
  [data-dir ledger startBlock endBlock opts]
  (let [blocks (get-block-range data-dir ledger startBlock endBlock)]
    (loop [[block & r] blocks
           expected-t nil                                   ;; expected first t value of block
           reports    (sorted-map)]
      (if-not block
        reports
        (let [[block-data checksum] (try (read-block+hash data-dir ledger block)
                                         (catch Exception _ nil)) ;; doesn't exist or decoding error
              flakes       (:flakes block-data)
              tx-map       (->> flakes                      ;; group all tx flakes by t
                                (filter #(< (.-s %) 0))     ;; all subject-ids < 0
                                (group-by #(.-s %)))
              t-list       (not-empty (sort > (keys tx-map)))
              block-t      (last t-list)
              block-flakes (get tx-map block-t)             ;; the last 't' value should be the block meta
              block-n      (some #(when (= (.-p %) constants/$_block:number) (.-o %)) block-flakes)
              instant      (some #(when (= (.-p %) constants/$_block:instant) (.-o %)) block-flakes)
              tx-map*      (dissoc tx-map block-t)          ;; remove block meta, should be left with only tx / commands
              tx-reports   (->> (vals tx-map*)              ;; a report for each tx returned as a map
                                (reduce (fn [acc t-flakes]
                                          (conj acc (tx-item-report block t-flakes))) []))
              warnings     (cond-> (keep :warnings tx-reports) ;; get any warnings from within individual txs
                                   ;; block data file not there or could not decode
                                   (nil? block-data)
                                   (conj (str "Block " block " is either missing or cannot be decoded."))

                                   ;; gap in 't' values for this block
                                   (and block-data expected-t (not= expected-t (first t-list)))
                                   (conj (str "Block " block " has a gap in 't' values. "
                                              "Expected block to start with t: " expected-t
                                              " but instead started with: " (first t-list) "."))
                                   ;; missing 't' inside the block
                                   (and t-list (range-gap t-list))
                                   (conj (str "Block " block " is has a missing t value of: " (range-gap t-list) ".")))
              report       (util/without-nils
                             {:file      (str data-dir (ledger-block-path* ledger block)) ;; output path of file we read
                              :block     block-n
                              :block-t   block-t
                              :instant   instant
                              :instant-t (when instant (str (java.time.Instant/ofEpochMilli instant)))
                              :tx-count  (when tx-reports (count tx-reports))
                              :txs       tx-reports
                              :warnings  (not-empty warnings)
                              :checksum  checksum           ;; sha-256 hash of file bytes
                              })
              next-t       (when block-t (dec block-t))]
          (recur r next-t (assoc reports block report)))))))


(defn ledger-compare*
  "Produces a map of blocks to found issues across multiple ledger server directories to find inconsistencies
  or issues.

  Checks for the following issues (for now):
  -- 't' value continuity (via tx-report)
  -- blocks that are missing in any of the data-dirs
  -- checksum comparison of block files, inconsistencies noted

  data-dirs is a list of valid data directories (directories should all have a trailing '/')"
  [ledger data-dirs opts]
  (let [dirs             (into #{} data-dirs)
        {:keys [start end]} opts
        reports          (reduce (fn [acc data-dir]         ;; map of each ledger directory (key) to ledger report and other info
                                   (let [report (tx-report data-dir ledger start end nil)]
                                     ;; want our output format to be by block, not by directory
                                     (reduce-kv (fn [acc* block data]
                                                  (assoc-in acc* [block data-dir] data)) acc report))) {} data-dirs)
        ;; helper fn that only returns a warnings from a block's aggregated report
        extract-warnings (fn [data-dirs] (->> data-dirs
                                              (reduce-kv (fn [acc data-dir data]
                                                           (if-let [warning (:warnings data)]
                                                             (assoc acc data-dir warning)
                                                             acc))
                                                         {})
                                              (not-empty)))
        ;; pull out any warnings from the tx-reports
        report-warnings  (->> reports
                              (keep (fn [[block data-dirs]]
                                      (when-let [warnings (extract-warnings data-dirs)]
                                        [block {:warnings warnings}])))
                              (into (sorted-map)))
        ;; add inconsistencies from the reports to the report-warnings
        output           (reduce-kv (fn [acc block block-dirs]
                                      (let [missing-blocks  (set/difference dirs (into #{} (keys block-dirs))) ;; identify any missing blocks
                                            ;; checksums map of {"data/directory" "checksum value"}
                                            checksums       (reduce-kv (fn [acc block-dir data]
                                                                         (assoc acc block-dir (:checksum data))) {} block-dirs)
                                            checksum-error? (apply not= (vals checksums))]
                                        (cond-> acc

                                                (not-empty missing-blocks)
                                                (assoc-in [block :missing-in] missing-blocks)

                                                checksum-error?
                                                (assoc-in [block :checksum-error] checksums))))
                                    report-warnings reports)]

    output))

(defn ledger-compare
  "If a glob character (*, ?) exists in the ledger name, compares multiple ledgers
  and puts all ledger comparisons within a map keyed by each matching ledger.

  Otherwise does a single ledger compare."
  [ledger data-dirs opts]
  (if (util/has-glob? ledger)
    (let [;; consolidate all ledgers across all dirs
          all-ledgers (reduce #(into %1 (glob-match-ledgers %2 ledger)) (sorted-set) data-dirs)]
      (reduce (fn [acc ledger]
                (println "Working on ledger:" ledger)
                (assoc acc ledger (ledger-compare* ledger data-dirs opts)))
              (sorted-map) all-ledgers))
    (ledger-compare* ledger data-dirs opts)))





