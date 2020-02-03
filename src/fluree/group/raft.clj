(ns fluree.group.raft
  (:require [fluree.util :as util]
            [fluree.raft.kv-example :as raft-kv]
            [fluree.raft.log :as raft-log]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.java.io :as io])
  (:import (java.util UUID)))

(defn dissoc-ks
  "Dissoc, but with a key sequence."
  [map ks]
  (if (= 1 (count ks))
    (dissoc map (first ks))
    (update-in map (butlast ks) dissoc (last ks))))

(defn assoc-in*
  [command state-atom]
  (let [[_ ks v] command]
    (if (nil? v)
      (swap! state-atom dissoc-ks ks)
      (swap! state-atom assoc-in ks v))
    true))

(defn get-in*
  [command state-atom]
  (let [[_ ks] command]
    (get-in @state-atom ks)))


(defn dissoc-in
  [map ks]
  (let [ks*        (butlast ks)
        dissoc-key (last ks)]
    (if ks*
      (update-in map ks* dissoc dissoc-key)
      (dissoc map dissoc-key))))

(defn cas-in
  [command state-atom]
  (let [[_ ks swap-v compare-ks compare-v] command
        new-state (swap! state-atom (fn [state]
                                      (let [current-v (get-in state compare-ks)]
                                        (if (= current-v compare-v)
                                          (assoc-in state ks swap-v)
                                          state))))]
    (= swap-v (get-in new-state ks))))

(defn max-in
  [command state-atom]
  (let [[_ ks proposed-v] command
        new-state (swap! state-atom
                         update-in ks
                         (fn [current-val]
                           (if (or (nil? current-val)
                                   (> proposed-v current-val))
                             proposed-v
                             current-val)))]
    (= proposed-v (get-in new-state ks))))

(defn register-new-dbs
  "Does not parse - assumes it is valid"
  [network dbid txns state-atom block-map]
  (swap! state-atom (fn [s]
                      (assoc-in s [:networks network :dbs dbid :status] :initialize))))

(defn stage-new-db
  [command state-atom]
  (let [[_ network dbid cmd-id new-db-command] command
        db-status {:status :initialize}]
    (if (get-in @state-atom [:networks network :dbs dbid])
      false                                                 ;; already exists
      (do
        (swap! state-atom (fn [s]
                            (-> s
                                (assoc-in [:networks network :dbs dbid] db-status)
                                (assoc-in [:new-db-queue network cmd-id] {:network network
                                                                          :dbid    dbid
                                                                          :command new-db-command}))))
        cmd-id))))

(defn delete-db
  [command state-atom]
  (let [[_ old-network old-db] command
        ;; dissoc all other values, set status to :deleted
        _ (swap! state-atom assoc-in [:networks old-network :dbs old-db] {:status :delete})
        ;; If we eventually decide to allow renaming dbs, we should ensure evenly distributed
        ;; networks after migration. For now, we don't delete network
        ]
    true))

(defn initialized-db
  [command state-atom]
  (let [[_ cmd-id network dbid status] command
        ok? (= :initialize (get-in @state-atom [:networks network :dbs dbid :status]))]
    (if ok?
      (do (swap! state-atom (fn [s]
                              (-> s
                                  (update-in [:networks network :dbs dbid] merge status)
                                  (assoc-in [:networks network :dbs dbid :indexes (:index status)] (System/currentTimeMillis))
                                  (dissoc-in [:new-db-queue network cmd-id]))))
          true)
      (do
        (swap! state-atom (fn [s]
                            (-> s
                                (dissoc-in [:new-db-queue network cmd-id]))))
        false))))

(defn new-index
  [command state-atom]
  (let [[_ network dbid index submission-server opts] command
        {:keys [status]} opts
        current-index    (get-in @state-atom [:networks network :dbs dbid :index])
        is-more-current? (if current-index
                           (> index current-index)
                           true)
        server-allowed?  (= submission-server
                            (get-in @state-atom [:_work :networks network]))]
    (if (and is-more-current? server-allowed?)
      (do
        (swap! state-atom update-in [:networks network :dbs dbid]
               (fn [db-data]
                 (-> db-data
                     (assoc :index index)
                     (assoc-in [:indexes index] (System/currentTimeMillis))
                     (assoc :status (or status :ready)))))
        true)
      false)))

(defn rename-keys-in-state
  [state-atom path]
  (let [networks     (-> (get-in @state-atom path) keys)
        new-networks (map str/lower-case networks)]
    (swap! state-atom update-in path set/rename-keys (zipmap networks new-networks))))

(defn lowercase-all-names
  [command state-atom]
  (let [;; Rename :networks
        _        (rename-keys-in-state state-atom [:networks])
        networks (->> (get @state-atom :networks) keys (map str/lower-case))
        ;; Update :dbs in :networks
        _        (mapv (fn [nw]
                         (let [dbs     (-> (get-in @state-atom [:networks nw :dbs]) keys)
                               new-dbs (map str/lower-case dbs)]
                           (swap! state-atom update-in [:networks nw :dbs] set/rename-keys (zipmap dbs new-dbs))))
                       networks)
        ;; Rename :db-queue
        _        (rename-keys-in-state state-atom [:new-db-queue])

        ;; Rename :cmd-queue
        _        (rename-keys-in-state state-atom [:cmd-queue])

        ;; Rename :_work
        _        (rename-keys-in-state state-atom [:_work :networks])

        servers  (-> (get-in @state-atom [:_worker]) keys)

        ;; Rename :_worker
        _        (mapv #(rename-keys-in-state state-atom [:_worker % :networks])
                       servers)]
    true))

(defn state-machine-only
  [state-atom]
  (fn [command]
    (let [op     (first command)
          result (case op

                   :new-block (let [[_ network dbid block-map submission-server] command
                                    {:keys [block txns cmd-types]} block-map
                                    current-block   (get-in @state-atom [:networks network :dbs dbid :block])
                                    txids           (keys txns)
                                    is-next-block?  (if current-block
                                                      (= block (inc current-block))
                                                      (= 1 block))
                                    server-allowed? (= submission-server
                                                       (get-in @state-atom [:_work :networks network]))]
                                (when (cmd-types :new-db)
                                  (register-new-dbs network dbid txns state-atom block-map))

                                (if (and is-next-block? server-allowed?)
                                  (swap! state-atom
                                         (fn [state]
                                           (-> (reduce (fn [s txid] (dissoc-in s [:cmd-queue network txid])) state txids) (assoc-in [:networks network :dbs dbid :block] block))))
                                  (swap! state-atom
                                         (fn [state]
                                           (reduce (fn [s txid] (dissoc-in s [:cmd-queue network txid])) state txids)))))


                   ;; stages a new db to be created
                   :new-db (stage-new-db command state-atom)

                   :delete-db (delete-db command state-atom)

                   :initialized-db (initialized-db command state-atom)

                   :new-index (new-index command state-atom)

                   :lowercase-all-names (lowercase-all-names command state-atom)

                   :assoc-in (assoc-in* command state-atom)

                   ; worker assignments are a little different in that they organize the key-seq
                   ; both prepended by the server-id (for easy lookup of work based on server-id)
                   ; and also at the end of the key-seq (for easy lookup of worker(s) for given resource(s))
                   ; all worker data is stored under the :_worker key
                   :worker-assign (let [[_ ks worker-id] command
                                        unassign? (nil? worker-id)
                                        work-ks   (into [:_work] ks)
                                        worker-ks (into [:_worker worker-id] ks)]
                                    (swap! state-atom
                                           (fn [state]
                                             (let [existing-worker (get-in state work-ks)]
                                               (if unassign?
                                                 (-> state
                                                     (dissoc-in work-ks)
                                                     (dissoc-in worker-ks))
                                                 (-> (if existing-worker
                                                       (dissoc-in state (into [:_worker existing-worker] ks))
                                                       state)
                                                     (assoc-in work-ks worker-id)
                                                     (assoc-in worker-ks (System/currentTimeMillis)))))))
                                    true)

                   :get-in (get-in* command state-atom)

                   ;; Returns true if there was an existing value removed, else false.
                   :dissoc-in (dissoc-in command state-atom)

                   ;; acquires lease, stored at specified ks (a more elaborate cas). Uses local clock
                   ;; to help combat clock skew. Will only allow a single lease at specified ks.
                   ;; returns true if provided id has the lease, false if other has the lease
                   :lease (let [[_ ks id expire-ms] command
                                epoch-ms     (System/currentTimeMillis)
                                expire-epoch (+ epoch-ms expire-ms)
                                new-lease    {:id id :expire expire-epoch}
                                new-state    (swap! state-atom update-in ks
                                                    (fn [current-lease]
                                                      (cond
                                                        ;; no lease, or renewal from current lease holder
                                                        (or (nil? current-lease) (= (:id current-lease) id))
                                                        new-lease

                                                        ;; a different id has the lease, not expired
                                                        (<= epoch-ms (:expire current-lease))
                                                        current-lease

                                                        ;; a different id has the lease, expired
                                                        :else
                                                        new-lease)))]
                            ;; true if have the lease
                            (= id (:id (get-in new-state ks))))

                   ;; releases lease if id is the current lease holder, or no lease exists. Returns true as operation always successful.
                   :lease-release (let [[_ ks id] command]
                                    (swap! state-atom
                                           (fn [state]
                                             (let [release? (or (nil? (get-in state ks))
                                                                (= id (:id (get-in state ks))))]
                                               (if release?
                                                 (dissoc-in state ks)
                                                 state))))
                                    true)

                   ;; Will replace current val at key sequence only if existing val is = compare value at compare key sequence.
                   ;; Returns true if value updated.
                   :cas-in (cas-in command state-atom)

                   ;; Will replace current val only if existing val is < proposed val. Returns true if value updated.
                   :max-in (max-in command state-atom)

                   ;; Ignore this command here
                   :storage-write true
                   ;; Ignore this command here
                   :storage-read true)]
      result)))

(defn log-files-to-commit
  [last-snapshot log-dir]
  (->> (raft-log/all-log-indexes log-dir "raft")
       sort
       (filter #(<= (or last-snapshot 0) %))))

(defn snapshot-reify-safe
  [snapshot-reify-fn latest-snapshot]
  (try (snapshot-reify-fn latest-snapshot)
       (catch Exception e false)))

(defn retrieve-latest-snapshot
  [snapshot-file-path snapshot-reify-fn]
  (let [last-snapshot (raft-log/latest-log-index snapshot-file-path "snapshot")
        reified       (snapshot-reify-safe snapshot-reify-fn last-snapshot)]
    (if reified
      [last-snapshot reified]
      (throw (ex-info (str "Unable to reify: " last-snapshot ".snapshot.
      Please create a backup of your log directory, and then delete snapshots/" last-snapshot ".snapshot
       Make sure that you know what your default private key is before you delete!")
                      {:status 400
                       :error  :db/invalid-snapshot})))))

(defn commit-log-index-to-state
  [log-dir log-index state-machine]
  (let [log-file (io/file (str log-dir log-index ".raft"))
        entries  (try (raft-log/read-log-file log-file)
                      (catch Exception e
                        (throw (ex-info (str "Unable to read log file: " log-index ".raft.
                        Please create a backup of your log directory, and then delete " log-index ".raft.
                        Also, delete the snapshot at this point (in the snapshots/ folder).
                        Make sure that you know what your default private key is before you delete!")
                                        {:status 400
                                         :error  :db/log-raft}))))]
    (loop [[entry & r] entries]
      (if-not entry
        true
        (if (= :append-entry (nth entry 2))
          (do (state-machine (-> entry (nth 3) :entry))
              (recur r))
          (recur r))))))

(defn generate-0-log
  [log-dir this-server]
  (let [_     (println (str "No raft logs found. Creating 0.raft."))
        raft0 (io/file (str log-dir "0.raft"))]
    (do
      (raft-log/write-current-term raft0 1)
      (raft-log/append raft0 [{:term  1
                               :entry [:lease [:leases :servers this-server] this-server 5000]
                               :id    (str (UUID/randomUUID))}] 1 1))))

(defn update-state-to-commit
  [state-atom config-atom]
  (let [log-dir           (:log-dir @config-atom)
        snapshot-dir      (str log-dir "snapshots/")
        snapshot-reify-fn (raft-kv/snapshot-reify snapshot-dir state-atom)
        [last-snapshot _] (retrieve-latest-snapshot snapshot-dir snapshot-reify-fn)
        files-to-commit   (log-files-to-commit last-snapshot log-dir)
        state-machine     (state-machine-only state-atom)]
    (if (and (not last-snapshot) (empty? files-to-commit))
      (do (generate-0-log log-dir (:this-server @config-atom))
          (update-state-to-commit state-atom config-atom))
      (loop [[index & r] files-to-commit]
        (if-not index
          true
          (do (commit-log-index-to-state log-dir index state-machine)
              (recur r)))))))

(defn latest-append-index-term
  [log-entries]
  (some #(when (= (nth % 2) :append-entry) [(first %) (second %)]) (reverse log-entries)))

(defn set-raft-log
  [log-dir command]
  (let [latest-log      (raft-log/latest-log-index log-dir "raft")
        latest-log-file (io/file log-dir (str latest-log ".raft"))
        log-entries     (raft-log/read-log-file latest-log-file)
        [latest-index latest-term] (latest-append-index-term log-entries)]
    ;file entries after-index current-index
    (raft-log/append latest-log-file [{:term  latest-term
                                       :entry command
                                       :id    (str (UUID/randomUUID))}]
                     latest-index latest-index)))

(defn get-network-ledgers
  [raft-state network]
  (let [dbs (-> @raft-state :networks (get network) :dbs keys)]
    (map #(str network "/" %) dbs)))

(defn get-dbs [raft-state]
  (let [networks (-> @raft-state :networks keys)
        dbs      (-> (map #(get-network-ledgers raft-state %) networks) flatten)]
    dbs))

(defn get-raft-state
  [raft-state config-atom]
  (do (swap! raft-state dissoc :loaded)
      (update-state-to-commit raft-state config-atom)))

(comment
  (def st-atom (atom {:loaded false}))
  (swap! st-atom dissoc :loaded)
  @st-atom

  )

(defn ensure-raft-state-loaded
  [raft-state config-atom]
  (when (false? (:loaded @raft-state)) (get-raft-state raft-state config-atom)))

(defn list-ledgers
  [raft-state config-atom args]
  (if args
    (let [network (first args)]
      (do (ensure-raft-state-loaded raft-state config-atom)
          (println (str "Ledgers: " (str/join ", " (get-network-ledgers raft-state (str network)))))))
    (do (ensure-raft-state-loaded raft-state config-atom)
        (println (str "Ledgers: " (str/join ", " (get-dbs raft-state)))))))

(defn ledger-info
  [raft-state config-atom args]
  (do (get-raft-state raft-state config-atom)
      (let [[nw ledger] (util/get-ledger-name args)
            info (get-in @raft-state [:networks nw :dbs ledger])]
        (println (str "Ledger info for " nw "/" ledger ": " info)))))

(defn forget-ledger
  [raft-state config-atom args]
  (do (ensure-raft-state-loaded raft-state config-atom)
      (if args
        (let [[nw ledger] (util/get-ledger-name args)
              nw-dbs        (get-network-ledgers raft-state nw)
              single-db     (= 1 (count nw-dbs))
              ledger-in-nw? ((set nw-dbs) (str nw "/" ledger))
              this-server   (get @config-atom :this-server)]
          (cond (not ledger-in-nw?)
                (throw (ex-info (str "Cannot forget ledger. No such ledger in state. Provided: " nw "/"
                                     ledger ". Network ledgers: " (str/join ", " nw-dbs))
                                {:status 400
                                 :error  :invalid-command}))
                ;; Forget network
                single-db
                ;; Assoc-in with no value is dissoc-in
                (do (set-raft-log (get @config-atom :log-dir) [:assoc-in [:networks nw]])
                    (set-raft-log (get @config-atom :log-dir) [:assoc-in [:new-db-queue nw]])
                    (set-raft-log (get @config-atom :log-dir) [:assoc-in [:cmd-queue nw]])
                    (set-raft-log (get @config-atom :log-dir) [:assoc-in [:_work :networks nw]])
                    (set-raft-log (get @config-atom :log-dir) [:assoc-in [:_worker this-server
                                                                          :networks nw]])
                    (get-raft-state raft-state config-atom)
                    (ledger-info raft-state config-atom [nw ledger]))

                :else
                ;; Only forget ledger, not network
                (do (set-raft-log (get @config-atom :log-dir) [:assoc-in [:networks nw :dbs ledger]])
                    (get-raft-state raft-state config-atom)
                    (ledger-info raft-state config-atom [nw ledger]))))

        (throw (ex-info (str "When forgetting a ledger, must provided a ledger name. Expected either `ledger network/ledger` or `network ledger`")
                        {:status 400
                         :error  :invalid-command})))))

(defn handle-key
  [key]
  (cond (string? key) key
        (symbol? key) (keyword key)
        (keyword? key) key))

(defn raft-state-api
  "`group-state` or `group-state get` will return the current group-state.

  `group-state keys` returns all the keys of the current group-state.

  `group-state [KEYNAME]`, for example `group-state private-key` will return the value of that key.

  group-state [ KEYNAME1 KEYNAME2 ], for example `group-state [networks \"fluree\" dbs]` will return the value at that key path. Note that keys that are listed without quotation marks, like `networks` above are treated as keyword, `:networks`. And those listed in quotation marks are treated as strings. Pay attention to the raft state map to see how the key path you are interested in is listed."
  [raft-state config-atom]
  (fn ([] (do (get-raft-state raft-state config-atom)
              (println "Raft state: " @raft-state)))

    ([arg] (do (if (= "get" (str arg))
                 (do
                   (get-raft-state raft-state config-atom)
                   (println "Raft state: " @raft-state))

                 (do (ensure-raft-state-loaded raft-state config-atom)
                     (if (= (str arg) "keys")
                       (println "Raft keys: " (keys @raft-state))

                       (let [vec?  (vector? arg)
                             value (if vec?
                                     (get-in @raft-state (map handle-key arg))
                                     (get @raft-state (handle-key arg)))]
                         (if value
                           (println (str "Raft value for " arg ": " value))
                           (println (str "The current Raft state does not contain the provided key: " arg)))))))))))

(defn ledger-raft-info
  [block index-point]
  (cond-> {:block block}
          index-point (merge {:status  :ready
                              :index   index-point
                              :indexes {index-point (System/currentTimeMillis)}})
          (not index-point) (merge {:status :reindex})))

(defn block-file-exists?
  [data-dir nw ledger block]
  (let [block-dir (str data-dir nw "/" ledger "/block/")
        blocks    (-> (raft-log/all-log-indexes block-dir "fdbd") set)]
    (blocks block)))

(comment

  (ledger-raft-info 4 nil)
  (raft-log/latest-log-index "/Users/plogian/fluree/fluree.db.transactor/data/ledger/fluree/new/root/" "fdbd")

  (def log-entries (raft-log/read-log-file (io/file "/Users/plogian/Downloads/fluree-cli-test/data/group/0.raft")))

  (count log-entries)

  log-entries
  (raft-log/append)

  )

(defn ledger-api
  "All possible commands: `get`, `ls`, `remember [LEDGER]`, `forget [LEDGER]`, `set-block [LEDGER] [BLOCK]`

  `get` and `ls` both list all the ledgers across all networks.
  `remember` and `forget` followed by a ledger name (either as `network ledger` or `network/ledger`) remember or forget a ledger, respectively. When remembering a ledger, we check the latest block in the block folder, and we set the block number accordingly. To set the ledger to a different block, use `set-block` (below).
  `set-block [NW/LEDGER or NW LEDGER] [BLOCK]` sets the latest block for a ledger. For example, `set-block fluree test 3`"
  [raft-state config-atom]
  (fn ([]
       (do (ensure-raft-state-loaded raft-state config-atom)
           (println (str "Ledgers: " (str/join ", " (get-dbs raft-state))))))
    ([command & args]
     (condp = (str command)
       "get" (list-ledgers raft-state config-atom args)
       "ls" (list-ledgers raft-state config-atom args)
       "info" (ledger-info raft-state config-atom args)
       "forget" (forget-ledger raft-state config-atom args)
       "remember" (do (ensure-raft-state-loaded raft-state config-atom)
                      (if args
                        (let [[nw ledger] (util/get-ledger-name args)
                              nw-exists?  ((-> @raft-state :networks keys set) nw)
                              log-dir     (get @config-atom :log-dir)
                              data-dir    (get @config-atom :data-dir)
                              block-dir   (str data-dir nw "/" ledger "/block/")
                              block       (raft-log/latest-log-index block-dir "fdbd")
                              root-dir    (str data-dir nw "/" ledger "/root/")
                              index-point (raft-log/latest-log-index root-dir "fdbd")]
                          (cond (not block)
                                (throw (ex-info (str "When remembering a ledger, block files must be in the storage directory. Cannot locate files for: " nw "/" ledger)
                                                {:status 400
                                                 :error  :invalid-command}))

                                nw-exists?
                                (do (set-raft-log log-dir [:assoc-in [:networks nw :dbs ledger]
                                                           (ledger-raft-info block index-point)])
                                    (ledger-info raft-state config-atom [nw ledger]))

                                ;; else need to create network
                                :else
                                (let [this-server (get @config-atom :this-server)]
                                  (do (set-raft-log log-dir [:assoc-in [:networks nw :dbs] {ledger {}}])
                                      (set-raft-log log-dir [:assoc-in [:new-db-queue nw] {}])
                                      (set-raft-log log-dir [:assoc-in [:cmd-queue nw] {}])
                                      (set-raft-log log-dir [:assoc-in [:_work :networks nw] this-server])
                                      (set-raft-log log-dir [:assoc-in [:_worker this-server :networks nw]
                                                             (+ (System/currentTimeMillis) 5000)])
                                      (set-raft-log log-dir [:assoc-in [:networks nw :dbs ledger]
                                                             (ledger-raft-info block index-point)])
                                      (ledger-info raft-state config-atom [nw ledger])))))
                        (throw (ex-info (str "When remembering a ledger, must provided a ledger name. Expected either `network/ledger` or `network ledger`")
                                        {:status 400
                                         :error  :invalid-command}))))
       "set-block" (do (ensure-raft-state-loaded raft-state config-atom)
                       (if (>= (count args) 2)
                         (let [[nw ledger] (util/get-ledger-name (drop-last args))
                               log-dir      (get @config-atom :log-dir)
                               data-dir     (get @config-atom :data-dir)
                               block        (last args)
                               block        (if (string? block) (read-string block) block)
                               block'       (if (and (int? block) (block-file-exists? data-dir nw ledger block))
                                              block
                                              (throw (ex-info (str "Invalid block. Provided: " block)
                                                              {:status 400
                                                               :error  :invalid-command})))
                               index-points (->> (raft-log/all-log-indexes (str data-dir nw "/" ledger "/root/")
                                                                           "fdbd")
                                                 (filter #(>= block' %)))
                               index-point  (if-not (empty? index-points)
                                              (apply max index-points)
                                              nil)]
                           (do (set-raft-log log-dir [:assoc-in [:networks nw :dbs ledger]
                                                      (ledger-raft-info block' index-point)])
                               (ledger-info raft-state config-atom [nw ledger])))

                         (throw (ex-info (str "When setting a block, must provided a network, ledger, and block. i.e. `ledger set-block mytest db 2` or `ledger set-block mytest/db 2`")
                                         {:status 400
                                          :error  :invalid-command}))))))))


(defn get-set-k-v-api
  "Used to get or set a top-level key in the RAFT state. Used for private-key and version"
  [raft-state config-atom k]
  (fn [& args]
    (do (get-raft-state raft-state config-atom)
        (let [set-or-get (when args (first args))
              set-or-get (if (nil? set-or-get)
                           nil (-> set-or-get str str/lower-case))
              original-k (get @raft-state k)
              k-str      (-> (util/keyword->str k)
                             (str/replace #"-" " "))]
          (cond
            (or (nil? set-or-get) (= "get" set-or-get))
            (do (println (str "Current " (if (= :private-key k) "default " "") k-str ": " original-k "."))
                (println (str "To change, use the command `" (util/keyword->str k) " set`")))

            (= "set" set-or-get)
            (if (= (count args) 2)
              (set-raft-log (get @config-atom :log-dir) [:assoc-in [k] (second args)])
              (let [_     (println (str "Current  " (if (= :private-key k) "default " "") k-str ": " original-k "."))
                    new-k (do (print (str "New " k-str " (enter to keep the old value): "))
                              (flush)
                              (read-line))
                    _     (println)
                    new-k (if (or (nil? new-k) (= "" (str/trim new-k)))
                            original-k (str/trim new-k))]
                (set-raft-log (get @config-atom :log-dir) [:assoc-in [k] new-k]))))))))

