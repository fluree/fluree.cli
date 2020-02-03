(ns fluree.core
  (:require [fluree.config.core :as config]
            [fluree.ledger.core :as ledger]
            [fluree.group.raft :as raft]
            [fluree.util :as util]
            [cli4clj.cli :as cli]
            [clojure.string :as str])
  (:gen-class))

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (let [jvm-arguments   (-> (util/jvm-arguments) :input util/jvm-args->map)
        properties-file (or (:fdb-properties-file jvm-arguments) "fluree_sample.properties")
        props           (util/read-properties-file properties-file)
        port            (if-let [port (-> props :fdb-api-port)]
                          (read-string port)
                          nil)
        config-state    {:port        port
                         :data-dir    (:fdb-storage-file-directory props)
                         :log-dir     (:fdb-group-log-directory props)
                         :this-server (:fdb-group-this-server props)}
        config-atom     (atom config-state)
        raft-state      (atom {:loaded false})]
    (println "\n \n /$$$$$$$$/$$                                       \n| $$_____/ $$                                       \n| $$     | $$ /$$   /$$  /$$$$$$  /$$$$$$   /$$$$$$ \n| $$$$$  | $$| $$  | $$ /$$__  $$/$$__  $$ /$$__  $$\n| $$__/  | $$| $$  | $$| $$  \\__/ $$$$$$$$| $$$$$$$$\n| $$     | $$| $$  | $$| $$     | $$_____/| $$_____/\n| $$     | $$|  $$$$$$/| $$     |  $$$$$$$|  $$$$$$$\n|__/     |__/ \\______/ |__/      \\_______/ \\_______/\n                                                    \n                                                    \n                                                    ")
    (cli/start-cli {:cmds          {
                                    ;; CONFIG
                                    ;; Config is inferred from the fluree_sample.properties file.
                                    ;; In case a user needs to edit this, `config get` and `config set` may be useful

                                    :config        {:fn              (config/config-api config-atom)
                                                    :short-info      "Set or view Config."
                                                    :long-info       "`config` or `config get` will return the current configuration. `config set` will let you set select keys."
                                                    :completion-hint "`set` or `get`"}

                                    ;; GROUP
                                    ;; The below commands all primarily deal with reading and manipulating the
                                    ;; consensus logs, currently only Raft is supported.

                                    :raft-state    {:fn              (raft/raft-state-api raft-state config-atom)
                                                    :short-info      "View last non-corrupted group state."
                                                    :long-info       "`raft-state` or `raft-state get` will return the current group state. `raft-state keys` returns all the keys of the current group-state. `raft-state [KEYNAME]`, for example `raft-state private-key` will return the value of that key.
                                                    `raft-state [ KEYNAME1 KEYNAME2 ]`, for example `raft-state [networks \"fluree\" dbs]` will return the value at that key path. Note that keys that are listed without quotation marks, like `networks` above are treated as keyword, `:networks`. And those listed in quotation marks are treated as strings. Pay attention to the raft state map to see how the key path you are interested in is listed."
                                                    :completion-hint "`get`, `keys`, a key name, or a key path in [ ]"}

                                    :ledger        {:fn              (raft/ledger-api raft-state config-atom)
                                                    :short-info      "View and update ledger info. Including forgetting and remembering ledgers, and setting ledger blocks."
                                                    :long-info       "`get` and `ls` both list all the ledgers across all networks.
                                                    `info [NW/LEDGER or NW LEDGER]` to get the ledger info from raft-state.

                                                  `remember` and `forget` followed by a ledger name (either as `network ledger` or `network/ledger`) remember or forget a ledger, respectively. When remembering a ledger, we check the latest block in the block folder, and we set the block number accordingly. To set the ledger to a different block, use `set-block` (below).
                                                  `set-block [NW/LEDGER or NW LEDGER] [BLOCK]` sets the latest block for a ledger. For example, `set-block fluree test 3`"
                                                    :completion-hint "`get`, `ls`, `info [LEDGER]`, `remember [LEDGER]`, `forget [LEDGER]`, `set-block [LEDGER] [BLOCK]`"}
                                    :network       {:fn              (fn ([] (do (raft/ensure-raft-state-loaded raft-state config-atom)
                                                                                 (println (str "Networks: "
                                                                                               (str/join ", " (-> @raft-state :networks keys))))))
                                                                       ([command] (if (#{"get" "ls"} (str command))
                                                                                    (do (raft/ensure-raft-state-loaded raft-state config-atom)
                                                                                        (println (str "Networks: "
                                                                                                      (str/join ", " (-> @raft-state :networks keys)))))
                                                                                    (throw (ex-info (str "Unknown command. Provided: " command)
                                                                                                    {:status 400
                                                                                                     :error :db/invalid-command})))))
                                                    :short-info      "View all networks."
                                                    :long-info       "No arguments. This commands returns all networks listed in the raft state."
                                                    :completion-hint "No args"}
                                    :version       {:fn              (raft/get-set-k-v-api raft-state config-atom :version)
                                                    :short-info      "Get or set data version in the raft-state. i.e. `version set 3` or `version get`"
                                                    :long-info       "Get or set data version in the raft-state. i.e. `version set 3` or `version get`"
                                                    :completion-hint "`version get` or `version set [INT]"}
                                    :private-key   {:fn              (raft/get-set-k-v-api raft-state config-atom :private-key)
                                                    :short-info      "Get or set default private key in the raft-state. i.e. `private-key get`"
                                                    :long-info       "Get or set default private key in the raft-state. i.e. `private-key get`. When setting a private key, should specify the hex-encoded private key. For example, `private-key set 745f3040cbfba59ba158fc4ab295d95eb4596666c4c275380491ac658cf8b60c`"
                                                    :completion-hint "`private-key get` or `private-key set [HEX-ENCODED KEY]"}

                                    ;; LEDGER
                                    ;; The below commands primarily deal with reading the data directory

                                    :block         {:fn              (fn [& args] (let [data-dir (str (util/format-path (:data-dir @config-atom)))
                                                                                        [[nw lid] block] (if (util/ledger-name? (first args))
                                                                                                           [(str/split (-> args first str) #"/")
                                                                                                            (second args)]
                                                                                                           [[(-> args first str) (-> args second str)]
                                                                                                            (nth args 2)])
                                                                                        meta?    (if (boolean? (last args))
                                                                                                   args true)]
                                                                                    (ledger/get-block data-dir nw lid block meta?)))
                                                    :short-info      "View the flakes for a given block. Optionally exclude metadata."
                                                    :long-info       "`block [NW/LEDGER or NW LEDGER] [BLOCK] [META?]`
                                                  View the flakes for a given block. Optionally exclude metadata. The `META?` flag is optional and defaults to true. "
                                                    :completion-hint "`For example, `block fluree/test 4 false`"}
                                    :auth          {:fn              (fn [& args] (let [data-dir (str (util/format-path (:data-dir @config-atom)))
                                                                                        [nw lid startBlock endBlock] (util/parse-nw-lid-start-end-args args)]
                                                                                    (ledger/get-auth data-dir nw lid startBlock endBlock)))
                                                    :short-info      "Returns the auth and authority for a given block or block range."
                                                    :long-info       "`auth [NW/LEDGER or NW LEDGER] [START BLOCK] [END BLOCK - optional]
                                                  Returns the auth and authority for a given block or block range. Results returned as a map where the keys are the blocks, and the values are a map containing auth and authority."
                                                    :completion-hint "`auth [NW/LEDGER or NW LEDGER] [START BLOCK] [END BLOCK - optional]`"}

                                    :verify-blocks {:fn              (fn [& args] (let [data-dir (str (util/format-path (:data-dir @config-atom)))
                                                                                        [nw lid startBlock endBlock] (util/parse-nw-lid-start-end-args args)]
                                                                                    (ledger/verify-blocks data-dir nw lid startBlock endBlock)))
                                                    :short-info      "Verify the hashes and signatures for a given range of blocks in a database."
                                                    :long-info       "`verify-blocks [NW/LEDGER or NW LEDGER] [START BLOCK -  optional] [END BLOCK - optional]`
                                                    If no blocks are provided, the command will verify all blocks. If a single block is provided, only that block will be verified. If submitting a range of blocks, the start and end block are included."
                                                    :completion-hint "`verify-blocks [NW/LEDGER or NW LEDGER] [START BLOCK -  optional] [END BLOCK - optional]`"}}
                    :allow-eval    false
                    :prompt-string "fluree✶ "})))






(comment
  (ledger/get-auth "/Users/plogian/Downloads/fluree-cli-test/data/ledger/" "fluree" "test" "2" 4)

  )

