use bitcoin::consensus::deserialize;
use clap::Parser;
use libbitcoinkernel_sys::{
    BlockManagerOptions, ChainType, ChainstateLoadOptions, ChainstateManager,
    ChainstateManagerOptions,
};
use lmdb::{DatabaseFlags, Environment, Transaction, WriteFlags};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

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

struct TxIndex {
    txid: String,
    block_height: i32,
    position_in_block: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct TxIndexEntry {
    block_height: i32,
    position_in_block: usize,
}

#[derive(Clone)]
struct BlockIndexInfo {
    block_height: i32,
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
    let path = Path::new("./txindex");
    fs::create_dir_all(path)?;

    // Set up the LMDB environment
    let env = Environment::new()
        .set_max_dbs(10)
        .set_map_size(10 * 1024 * 1024 * 1024) // Increase map size to 10 GB
        .open(path)?;

    // Create (or open) a database
    let db = env.create_db(Some("txindex"), DatabaseFlags::empty())?;

    // Collect block indices
    let mut block_index_res = chainman.get_block_index_tip();
    let mut block_indices = Vec::new();
    while let Ok(ref block_index) = block_index_res {
        let block_height = block_index.info().unwrap().clone().height;
        block_indices.push(BlockIndexInfo { block_height });
        block_index_res = block_index_res.unwrap().prev();
    }

    // Process blocks in parallel
    let batch_size = 1000;
    block_indices.par_chunks(batch_size).for_each(|chunk| {
        let env = &env;
        let db = db;

        let tx_batch: Vec<TxIndex> = chunk
            .par_iter()
            .flat_map(|block_info| {
                let block_index = chainman
                    .get_block_index_by_height(block_info.block_height)
                    .unwrap();
                let raw_block: Vec<u8> = chainman.read_block_data(&block_index).unwrap().into();
                let block: bitcoin::Block = deserialize(&raw_block).unwrap();

                (0..block.txdata.len() - 1)
                    .map(|i| {
                        let txid = block.txdata[i + 1].compute_txid();
                        TxIndex {
                            txid: txid.to_string(),
                            position_in_block: i,
                            block_height: block_info.block_height,
                        }
                    })
                    .collect::<Vec<TxIndex>>()
            })
            .collect();

        let mut txn = env.begin_rw_txn().unwrap();
        for entry in tx_batch.iter() {
            let v = TxIndexEntry {
                position_in_block: entry.position_in_block,
                block_height: entry.block_height,
            };
            let serialized = bincode::serialize(&v).unwrap();
            txn.put(db, &entry.txid, &serialized, WriteFlags::empty())
                .unwrap();
        }
        txn.commit().unwrap();
    });

    log::info!("Built index!");

    // Example retrieval of a transaction's block location
    {
        let txid = "37d704c8550bf80213ed1b1c3b5798665c7274d67c707bc6e9d6eb4167d3b7f3".to_string();
        let txn = env.begin_ro_txn()?;
        if let Some(data) = txn.get(db, &txid).ok() {
            let txindex: TxIndexEntry = bincode::deserialize(data)?;
            println!(
                "Transaction ID: {}, Block Location: {}",
                &txid, txindex.position_in_block
            );
            let Ok(ref block_index) = chainman.get_block_index_by_height(txindex.block_height)
            else {
                todo!()
            };
            let raw_block: Vec<u8> = chainman.read_block_data(&block_index).unwrap().into();
            let block: bitcoin::Block = deserialize(&raw_block).unwrap();
            let tx = &block.txdata[txindex.position_in_block];
            println!("Full transaction: {:#?}", tx);
        }
    }

    Ok(())
}
