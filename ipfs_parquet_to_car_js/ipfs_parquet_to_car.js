import parquetjs from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { Buffer } from 'buffer';
import { Readable } from 'stream'
import { CarReader, CarWriter } from '@ipld/car'
import * as raw from 'multiformats/codecs/raw'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'

export class ipfsParquetToCarJs{
    constructor(resources, metadata){
        this.resources = resources;
        this.metadata = metadata;
    }

    async convert_to_car(parquet_file){
        try{
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            const parquet_path = path.join(this_dir, parquet_file);
            const parquet = await parquetjs.ParquetReader.openFile(parquet_path);
            const cursor = parquet.getCursor();
            let record = null;
            while (record = await cursor.next()) {
                console.log(record);
            }
        }catch(e){
            console.log(e);
        }

    }

    async convert_to_parquet(){

    }

    async test(){
        console.log("Hello from ipfs_parquet_to_car.js");
        let parquet_file = "bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet";
        try{
            await this.convert_to_car(parquet_file);
        }
        catch(e){
            console.log(e);
        }

    }
}
export default ipfsParquetToCarJs;

const testIpfsParquetToCarJs = new ipfsParquetToCarJs();
testIpfsParquetToCarJs.test();
