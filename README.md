# Fluree Command Line Interface - Beta

 The Fluree Command Line Interface (CLI) is a terminal-based tool that allows users to read and verify Fluree ledger files and consensus logs. Without running a Fluree node, this tool allows users to:
  
  - explore the blocks in a Fluree ledger, 
  - see who issued the transactions within a block, 
  - verify that the signatures and hashes in a block are valid,
  - view the network state, and more.
  
Manipulating logs (for example, setting a new default private key) is only recommended when running Fluree in a centralized fashion (as a single server). Changing Raft logs in a decentralized network is not recommended unless you know what you are doing. If you are going to change logs, please reach out to us on Slack or via email if you need help. To change logs, make sure that you are NOT running a Fluree node when running this CLI. To use this CLI, shut down your Fluree node, start up the CLI, make any changes that you need to, and start up your node once again. 

Note that this tool is a beta-version. If you run into issues, you can send us an email, support@flur.ee, or [join on our Slack channel](https://launchpass.com/flureedb).

#### Using the CLI

To download the latest version: [https://fluree-cli-releases-public.s3.amazonaws.com/fluree_cli-latest.zip](https://fluree-cli-releases-public.s3.amazonaws.com/fluree_cli-latest.zip).

To download a specific version: `https://fluree-cli-releases-public.s3.amazonaws.com/fluree_cli-${VERSION}.zip`, for example [https://fluree-cli-releases-public.s3.amazonaws.com/fluree_cli-0.1.0.zip](https://fluree-cli-releases-public.s3.amazonaws.com/fluree_cli-0.1.0.zip). 

Put the `fluree_cli.jar` in the folder where your Fluree network is running, and issue `java -jar fluree_cli.jar` to run the CLI. 

#### Versions

The Fluree CLI is designed to work with Fluree data from versions 0.11.X and higher. The CLI may work with previous versions, but this is not supported, and users should do this at their own risk (i.e. creating a copy of all files before experimenting with the CLI). 

## Commands

The Fluree CLI has two main types of commands:

- Ledger commands
- Group commands

**Ledger Commands**

Ledger commands deal with reading Fluree block files. These commands allow a users to do things like view the data and metadata within each block and verify the hashes and signatures in a block. 

**Group Commands** 

Group commands deal with reading and manipulating consensus logs. Currently only Raft logs are supported. For example, you are able to read the current Raft state, set a new default private key, and rebuild Raft state in the event of Raft log deletion. 

There is also a `config` command, which allows you to view and change some configuration details:

### config


To get the current config, you can issue `config` or `config get`. This will return a map with a port, data directory, log directory, and server name. 

For example:

`{:port 8080, :data-dir data/ledger/, :log-dir data/group/, :this-server myserver}`

To change the config, issue `command set`, which will prompt you to change the config settings. 


## Ledger Commands


### block

This command allows you to view the flakes for a given block, and optionally exclude metadata. By default, metadata flakes are included. The template for this command is:

`block LEDGER-NAME BLOCK [META? - optional]`

For example:

`block fluree/test 2 false`. 

This will return a map with the following keys: `block`, `t`, and `flakes`. 

### auth

This command returns the auth and authority subject ids for a given block or block range. The template for this command is:

`auth LEDGER-NAME START-BLOCK [END BLOCK - optional]`
                                                               
Results returned as a map where the keys are the blocks, and the values are a map containing `auth` and `authority`.

For example, the command `auth fluree/test 2 3` might return a map like:

`{2 {:auth 105553116266496, :authority nil}, 3 {:auth 105553116266496, :authority nil}}`


### verify-blocks

This command will verify the hashes and signatures for a given range of blocks in a database. The template for this command is:

`verify-blocks LEDGER-NAME [START BLOCK] [END BLOCK - optional]`

If a single block is provided, only that block will be verified. If submitting a range of blocks, the start and end block are included. 

For example, the command `verify-blocks fluree test 1 5` may return:


```
{:allValid true,

// Notice that block 1 does not have a previous hash or signature.

 1 {:prevHashMatch? nil,
    :hashValid? true,
    :hash "87ba49ef8f9a07b18f79e1b30ca231223863b48a37986b1d0cf7813dec29a96c",
    :signature nil,
    :signingAuth nil},
 2 {:prevHashMatch? true,
    :hashValid? true,
    :hash "b3c851951edd138fbed151bad42eb4917f96297fd03e119a6d06a963b7c9ff79",
    :signature "1b3045022100ff0a7a08a8a938998e485c6452fb3b74feda4562746dba9a456195f72a3a9aef02201300045ed5c448c5f7c519b9eccf0c84ae92c7a873f59ec75706cd301a73bfe4",
    :signingAuth "TexTgp1zpMkxJq1nThrgwkU5dp9wzaXA7BX"},
 3 {:prevHashMatch? true,
    :hashValid? true,
    :hash "988d17bdb90fb73c429c3b7d2fa0c3ade55e9ae6dcf1822912fc8d99b5f41f45",
    :signature "1c3045022100bca30abbc918a84b1d1a19f872ca24d59d13934b0e2402ffe623d7caa908885b0220221ffbe38baa5cfdf719bab9593a20c2d63e00922b5b4229e58dc11f1ea73eac",
    :signingAuth "TexTgp1zpMkxJq1nThrgwkU5dp9wzaXA7BX"},
 4 {:prevHashMatch? true,
    :hashValid? true,
    :hash "f5653a146b3c566c12b967a7aa17e6e7d43cd242e05d4fe73afb6a19938c6bbd",
    :signature "1b304402201ea9db7e494fc9f6d737148f3ca8cdff7bf08c21b90353998503d673be23537e02204a635223806653ddabf19f265d31618247d9d911ec83e914cad049917be0019a",
    :signingAuth "TexTgp1zpMkxJq1nThrgwkU5dp9wzaXA7BX"},
 5 {:prevHashMatch? true,
    :hashValid? true,
    :hash "c79a95ddbe81f9f844268f5b7112c553632b9ed0440d1a2502bec3bc6f54a8e4",
    :signature "1c3045022100fbdcaafa72c0e7844d615f0c74976692d64c4408b2b19e290c5576e8105a32a20220449642702f3ea0e652773648d08f445faec5d0028c1995a4089911523a39addf",
    :signingAuth "TexTgp1zpMkxJq1nThrgwkU5dp9wzaXA7BX"}
    
}
```

For each block, the keys in the block map are `prevHash`, `hash`, `signature`, `prevHashMatch?`, `hashValid?`, `sigValid?`.

### tx-report

This command provides a detailed report that analyzes all transactions across specified blocks. The template for this command is:

`tx-report LEDGER-NAME [START-BLOCK - optional] [END-BLOCK - optional]  [--data-dir=some/path/ - optional] [--output-file=myreport.json - optional]`

If no blocks are provided, all blocks that can be found on disk will be analyzed. If start and/or end blocks are provided the analysis will start or end at the specified block (inclusive).

The command will use the configured data-dir (type `config` to view or set default data directory). Optionally a data directory can be provided with the command using `--data-dir=/path/to/my/data/directory/`.

The output is returned to the console. Optionally it can be saved to a JSON file on disk by providing `--output-file=myreport.json`.

### ledger-compare

This command analyzes multiple data directories for multiple ledger servers that were used in a raft network to help find any inconsistencies. The template for this command is:

`ledger-compare LEDGER-GLOB DATA-DIR-1 DATA-DIR-2 [DATA-DIR-N ... optional] [--output-file=myreport.json - optional] [--start=10 - optional] [--end=20 - optional]`

Multiple ledgers can be compared by using glob characters `*` or `?`. For example `ledger-compare nw/* /tmp/ledger_1/data/ /tmp/ledger_2/data/ /tmp/ledger_3/data/` would produce a report for all ledgers starting with `nw/`. A `*` alone would produce the analysis for all ledgers.

The range of blocks analyzed can be narrowed by using the optional `--start=10 --end=42` which would only examine blocks 10 through 42 inclusive.

The output is returned to the console. Optionally it can be saved to a JSON file on disk by providing `--output-file=myreport.json`.

Output is a map/object with block numbers as keys where discrepencies occured. An empty map/object output of `{}` indicates there were zero discrepencies found. To see output of ledger data regardless of discrepencies, use the `tx-report` command, which this command leverages for its comparisons.

## Group Commands

### raft-state

The command `raft-state` or `raft-state get` will return the current group-state. The `raft-state` is retrieved from the configured log directory. First, the most recent non-corrupted snapshot is retrieved. Then all of the logs from the snapshot forward are applied to the state. If a raft state cannot be retrieved, there will be an error message to guide you to correct the issue. See `Scenarios` for more information on specific scenarios that may occur. 

`raft-state keys` returns all the keys of the current group state. For example, this will return a sequence like `(:version :leases :private-key :networks :new-db-queue :_work :_worker :cmd-queue)`. 

`raft-state [KEYNAME]`, for example `raft-state private-key` will return the value of that key.

`raft-state [ KEYNAME1 KEYNAME2 ]`, for example `raft-state [networks "fluree" dbs]` will return the value at that key path. Note that keys that are listed without quotation marks, like `networks` above are treated as keyword, `:networks`. Keywords, `:networks`, are also treated like keywords. Keys listed in quotation marks are treated as strings. Pay attention to the raft state map to see how the key path you are interested in is listed.   
  
### ledger

`ledger`, `ledger get` and `ledger ls` both list all the ledgers across all networks. This may return, for example, `Ledgers: fluree/test, new/one`.

`ledger info LEDGER` gets all the ledger info from raft-state, including most recent block and most recent index. For example, you could issue `ledger info fluree/test`, and that might return `Ledger info for fluree/test: {:status :ready, :block 4, :index 1, :indexes {1 1580497770328}}`. 

 `ledger remember` and `ledger forget` followed by a ledger name remember or forget a ledger, respectively. Make sure that you are running Fluree centralized (as a single server), and that the Fluree node is turned * off *. 
 
There are two components to the file system in a Fluree ledger:

- The group state, which is stored, by default in `data/group/`
- The actual ledger files, such as block files and index files. By default, this is stored in `data/ledger`.

#### Forgetting a Ledger

When you are forgetting a ledger, you are removing that database from the group state (the RAFT state in this case). You are not deleting the actual ledger files, such as a block files and index files. This means, that even after you forget a ledger, you can still remember that ledger later, copy the files and remember it in on a different network, or even just keep it as a historical record, without making updates to it later. 

The template for forgetting a ledger is `ledger forget LEDGER` 

For example, to forget the ledger `fluree/test`, you would issue `ledger forget fluree/test`.
  
#### Remembering a Ledger

When remembering a ledger, you must have that ledger's block and index files in the proper place in your directory. This means inside `data/ledger`, unless you have changed the default configuration for `fdb-storage-file-directory`. The ledger's block and index points are set to the latest block or index, respectively, that can be found in the ledger's folder.

#### Setting a Block

When we remember a ledger, we check the latest block in the block folder, and we set the block number accordingly. To set the ledger to a different block (including a valid index point at that block), use `set-block`.

`set-block LEDGER BLOCK` sets the latest block for a ledger. For example, `set-block fluree/test 3`.

This command will fail if the block files do not exist.

If the block files do not exist, but there are no indexes that are valid for this block, then the status of the database will be set to `reindex`, and you must issue a request to the `reindex` endpoint (see [API section in docs](https://docs.flur.ee/api/downloaded-endpoints/overview), looking at version 0.11.x or higher for more information). 

### Network

This command takes no arguments. `network` or `network ls` or `network get` all list the networks that are known by the Raft state. A network is what you see in the namespace portion of a ledger name, i.e. `my/ledger` is in the `my` network.

### Version

`version get` retrieves the data version in the Raft state. This is a equivalent to `raft-state version`.

Only set the data version in the Raft state if you know what you are doing. Make sure you running Fluree centralized (as a single server), and make sure that your Fluree node is NOT running. Then you can issue `version set [INT]`, for example `version set 3`. This command does NOT issue the upgrade script to upgrade a ledger from one version to another. But if, for example, you are remembering a ledger from a different network (that may be running an older version of Fluree), this command may be helpful. We recommend you reaching out to use via email or Slack if you run into a situation like this. 

### Private Key

`private-key get` retrieves the default private key from the Raft state. This is a equivalent to `raft-state private-key`.

 Make sure you running Fluree centralized (as a single server), and make sure that your Fluree node is NOT running. Then you can issue `private-key set [HEX-ENCODED PRIVATE KEY]`, for example `private-key set 745f3040cbfba59ba158fc4ab295d95eb4596666c4c275380491ac658cf8b60c`. This will change the default private key used to sign transactions. Before changing the default private key, make sure that the auth record associated with this private key is valid and has roles in the database (see the [Identity section](https://docs.flur.ee/docs/identity) in the docs for more information).
