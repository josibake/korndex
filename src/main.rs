use lmdb::{Environment, Database, WriteFlags, Transaction, DatabaseFlags};
use serde::{Serialize, Deserialize};
use std::fs;
use libbitcoinkernel_sys::{
    BlockManagerOptions, ChainType, ChainstateLoadOptions, ChainstateManager,
    ChainstateManagerOptions,
};
use bitcoin::Transaction as BitcoinTx;
use clap::Parser;
use log::info;
use std::sync::Arc;
use std::sync::Mutex;
use bitcoin::consensus::deserialize;

mod kernel;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Data directory
    #[arg(long)]
    datadir: String,

    /// Network
    #[arg(long)]
    network: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TxIndex {
    txid: String,
    block_height: u64,
    position_in_block: usize,
}
struct TxIndexEntry {
    block_height: u64,
    position_in_block: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let chain_type = match args.network.to_lowercase().as_str() {
        "mainnet" => ChainType::MAINNET,
        "testnet" => ChainType::TESTNET,
        "regtest" => ChainType::REGTEST,
        "signet" => ChainType::SIGNET,
        _ => {
            eprintln!("Invalid network type: {}", args.network);
            std::process::exit(1);
        }
    };
    let data_dir = args.datadir;
    let blocks_dir = data_dir.clone() + "/blocks";
    // Set up the kernel
    let _ = kernel::setup_logging().unwrap();
    let context = kernel::create_context(chain_type);
    let chainman = ChainstateManager::new(
        ChainstateManagerOptions::new(&context, &data_dir).unwrap(),
        BlockManagerOptions::new(&context, &blocks_dir).unwrap(),
        &context,
    )
    .unwrap();
    chainman
        .load_chainstate(ChainstateLoadOptions::new())
        .unwrap();
    chainman.import_blocks().unwrap();

    // Create directory for the LMDB environment
    let path = "./txindex";
    fs::create_dir_all(path)?;

    // Set up the LMDB environment
    let env = Environment::new()
        .set_max_dbs(10)
        .open(path)?;

    // Create (or open) a database
    let db = env.create_db(Some("txindex"), DatabaseFlags::empty())?;

    // Add transactions to the database
    {
        let mut block_index_res = chainman.get_block_index_tip();
        let mut block_counter = 0;

        let txn = env.begin_rw_txn()?;
        let txn = Arc::new(Mutex::new(txn));

        while let Ok(ref block_index) = block_index_res {
            let raw_block: Vec<u8> = chainman.read_block_data(&block_index).unwrap().into();
            let block: bitcoin::Block = deserialize(&raw_block).unwrap();

            let transactions_data: Vec<TxIndex> = (0..block.txdata.len() - 1)
                .map(|i| {
                    let txid: BitcoinTx = block.txdata[i + 1].compute_txid().to_string();
                    TxIndex {
                        txid: txid,
                        position_in_block: i,
                        block_height: 1,
                    }
                })
                .collect();



            transactions_data.par_iter().for_each(|data| {
                let entry = TxIndexEntry {
                    position_in_block: data.position_in_block,
                    block_height: data.block_height,
                };
                let serialized = bincode::serialize(&entry);
                txn.put(db, &data.txid, &serialized, WriteFlags::empty());

            });

            block_index_res = block_index_res.unwrap().prev();
            block_counter += 1;
            if block_counter % 10 == 0 {
                info!("Processed block number: {}", block_counter);
            }
        }
        let txn = Arc::try_unwrap(txn).expect("Failed to unwrap Arc").into_inner().unwrap();
        txn.commit()?;
        log::info!("built index!");
    }

    // Example retrieval of a transaction's block location
    {
        let txid = "example_txid".to_string();
        let txn = env.begin_ro_txn()?;
        if let Some(data) = txn.get(db, &txid).ok() {
            let txindex: TxIndex = bincode::deserialize(data)?;
            println!("Transaction ID: {}, Block Location: {}", txindex.txid, txindex.position_in_block);

            // Here you would use the block location to read the block and extract the full transaction
        }
    }

    Ok(())
}
