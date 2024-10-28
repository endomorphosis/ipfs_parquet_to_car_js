#!/usr/bin/env node
export * from './ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';
import { ipfsParquetToCarJs } from './ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';
export { ipfsParquetToCarJs } from './ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';
export { ipfsParquetToCarJs as default } from './ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';
export * from './test/test.js';
export { test_parquet_to_car_js } from './test/test.js';
import { test_parquet_to_car_js } from './test/test.js';
const test = new test_parquet_to_car_js();
export { test };
const ipfs_parquet_to_car_js = new ipfsParquetToCarJs();
export { ipfs_parquet_to_car_js };
import { fileURLToPath } from 'url';
import { dirname } from 'path';
if (import.meta.url === 'file://' + process.argv[1]) {
    let help = "Usage: node index.js input_file output_file\n";
    if(process.argv.length > 2){
        let input_file = process.argv[2];
        let output_file = process.argv[3];

        const this_dir = dirname(fileURLToPath(import.meta.url));
        if (input_file.startsWith('./')){
            input_file = this_dir + '/' + input_file;
        }
        if (output_file.startsWith('./')){
            output_file = this_dir + '/' + output_file;
        }
        if (input_file.startsWith("/")){
            input_file = input_file
        }
        if (output_file.startsWith("/")){
            output_file = output_file
        }
        if (!input_file.startsWith("./") && !input_file.startsWith("/")){
            input_file = this_dir + '/' + input_file;
        }
        if (!output_file.startsWith("./") && !output_file.startsWith("/")){
            output_file = this_dir + '/' + output_file
        }
        if (input_file.endsWith('.parquet') && output_file.endsWith('.car')){
            ipfs_parquet_to_car_js.convert_parquet_to_car(input_file, output_file);
        }
        if (input_file.endsWith('.car') && output_file.endsWith('.parquet')){
            ipfs_parquet_to_car_js.convert_car_to_parquet(input_file, output_file);
        }
        else{
            console.log(help);
        }
    }
    else{
        console.log(help);
    }
}