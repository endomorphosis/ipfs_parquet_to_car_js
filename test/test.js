import { ipfsParquetToCarJs } from '../ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';

export class test_parquet_to_car_js{
    constructor(){
        this.ipfsParquetToCarJs = new ipfsParquetToCarJs();
    }

    async init(){

    }

    async test(){
        console.log("Hello from test.js");
        try{
            await this.ipfsParquetToCarJs.test();
        }
        catch(e){
            console.log(e);
        }
        return "Test complete";
    }
}


if (import.meta.url === 'file://' + process.argv[1]) {
    console.log("Running test");
    let test_results = {};
    try{
        const test = new test_parquet_to_car_js();
        test_results = await test.test();
    }
    catch(e){
        console.log(e);
    }
    console.log(test_results);
}