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
           (java.net URI)
           (java.nio.file Files Path Paths StandardCopyOption CopyOption LinkOption)
           (java.nio.file.attribute FileAttribute)))

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

(defn block-filename
  "Returns a formatted filename for a block integer"
  [block]
  (str (format "%015d" block) ".fdbd"))

(defn ledger-block-path*
  [ledger block]
  (str ledger "/block/" (block-filename block)))

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


(defn copy-block
  [ledger data-dir repair-dir block]
  (let [blockfile   (block-filename block)
        src-path    ^Path (Paths/get data-dir (into-array [ledger "block" blockfile]))
        dest-path   ^Path (Paths/get repair-dir (into-array [ledger "block" blockfile]))
        dest-dir    (Paths/get repair-dir (into-array [ledger "block"]))
        dir-exists? (Files/exists dest-dir (into-array LinkOption []))]
    (when-not dir-exists?
      (Files/createDirectories dest-dir (into-array FileAttribute [])))
    (Files/copy src-path dest-path (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))))


(defn copy-block
  [ledger data-dir repair-dir block]
  (let [blockfile   (block-filename block)
        source      (io/file data-dir ledger "block" blockfile)
        destination (io/file repair-dir ledger "block" blockfile)]
    (io/make-parents destination)
    (io/copy source destination)))


(defn ledger-repair
  "For use with multiple ledger data-directories (that must be saved locally) which might have an inconsistency.
  Creates a new ledger directory (at :repair-dir option, or defaults to repaired/ledger) with all of the complete
  block files, and produces a report at the end as to what was repaired/ignored.

  If it is unable to have a consistent ledger directory, will throw.

  This will not copy over index files. If there is a complete ledger (final report will provide this info)
  the index files (and block files) can be used from that ledger. If there is not a complete ledger and it
  must be pieced together from the multiple ledger directories, you will need to re-index.

  Provide:
  - ledger - ledger name, i.e. my/ledger
  - data-dirs - list of the data directories of the ledger servers to compare
  - opts - currently only :repair-dir, which is the location to put the repaired ledger block files."
  [ledger data-dirs opts]
  (let [ledger-compare-report (ledger-compare* ledger data-dirs nil)
        {:keys [repair-dir] :or {repair-dir "repaired/ledger/"}} opts
        max-block             (apply max (map #(max-block-from-disk % ledger) data-dirs))
        blocks                (range 1 (inc max-block))
        ledger-stats          (atom (reduce #(assoc-in %1 [:good %2] 0) {} data-dirs))
        ledger-stats-update   (fn [ledgers]
                                (swap! ledger-stats
                                       (fn [stats]
                                         (reduce #(update-in %1 [:good %2] inc) stats ledgers))))
        ledger-stats-errors   (fn [block error ledgers]
                                (swap! ledger-stats
                                       (fn [stats]
                                         (reduce #(assoc-in %1 [:bad %2 block] error) stats ledgers))))]
    (doseq [block blocks]
      (let [block-info (get ledger-compare-report block)]
        (cond
          ;; no errors reported, can copy over ledger from any directory
          (nil? block-info)
          (do
            (copy-block ledger (first data-dirs) repair-dir block)
            (ledger-stats-update data-dirs))

          ;; ledgers differ, if we have two or more that agree we'll use those
          (:checksum-error block-info)
          (let [ledger-checksums      (:checksum-error block-info)
                checksum-counts       (reduce-kv (fn [acc _ checksum] (update acc checksum (fnil inc 0))) {} ledger-checksums)
                most-common-n         (apply max (vals checksum-counts))
                checksums-with-max    (keep #(when (= most-common-n (val %)) (key %)) checksum-counts)
                target-checksum       (first checksums-with-max)
                ledgers-with-checksum (into #{} (keep #(when (= target-checksum (val %))
                                                         (key %)) ledger-checksums))]
            (when (= 1 most-common-n)
              (throw (ex-info (str "Ledger: " ledger " block: " block " has no common checksums: " ledger-checksums
                                   "Unable to resolve!") {})))
            (when (not= 1 (count checksums-with-max))
              (throw (ex-info (str "Ledger: " ledger " block: " block " has multiple checksum conflicts: " ledger-checksums
                                   "Unable to resolve!") {})))
            ;; found a common checksum, copying
            (println ledger "block" block "checksum-match with" most-common-n "ledgers, copying block from:" (first ledgers-with-checksum))
            (copy-block ledger (first ledgers-with-checksum) repair-dir block)
            (ledger-stats-update ledgers-with-checksum)
            (ledger-stats-errors block :checksum-error (filter #(not (ledgers-with-checksum %)) data-dirs)))

          ;; block 't' value likely missing, see if a ledger has no warning for this block
          (:warnings block-info)
          (let [ledger-warning-info      (:warnings block-info)
                ledger-with-warnings     (into #{} (keys ledger-warning-info))
                ledgers-without-warnings (filter #(not (ledger-with-warnings %1)) data-dirs)]
            (when (empty? ledgers-without-warnings)
              (throw (ex-info (str "Ledger: " ledger " block: " block " has no ledgers without issues: " ledger-warning-info
                                   "Unable to resolve!") {})))
            (println ledger "block" block "warnings for all but" (count ledgers-without-warnings) "ledgers, copying block from:"
                     (first ledgers-without-warnings) "Warnings:" ledger-warning-info)
            (copy-block ledger (first ledgers-without-warnings) repair-dir block)
            (ledger-stats-update ledgers-without-warnings)
            (doseq [ledger ledger-with-warnings]
              (ledger-stats-errors block (get ledger-warning-info ledger) [ledger])))

          ;; block is missing in one or more ledgers
          (:missing-in block-info)
          (let [missing-in     (:missing-in block-info)
                not-missing-in (filter #(not (missing-in %)) data-dirs)]
            (when (empty? not-missing-in)
              (throw (ex-info (str "Ledger: " ledger " block: " block " has no ledgers with block: " block
                                   "Unable to resolve!") {})))
            (println ledger "block" block "missing in ledgers" missing-in ". Copying from:" (first not-missing-in))
            (copy-block ledger (first not-missing-in) repair-dir block)
            (ledger-stats-update not-missing-in)
            (ledger-stats-errors block :missing missing-in))


          :else
          (throw (ex-info (str "Ledger: " ledger " block: " block " Unexpected Error!: " block-info) {})))))
    (let [good-blocks  (get @ledger-stats :good)
          good-ledgers (filter #(= max-block (get good-blocks %1)) data-dirs)]
      (if (empty? good-ledgers)
        (println ">>>> No Ledger was complete!!!!")
        (println ">>>> Ledger(s)" good-ledgers "were complete!"))
      @ledger-stats)))





