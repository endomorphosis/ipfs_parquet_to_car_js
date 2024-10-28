import parquetjs from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';
import { Readable } from 'stream';
import { CarReader, CarWriter } from '@ipld/car';
import * as Block from 'multiformats/block';
import * as codec from '@ipld/dag-cbor';
import { sha256 } from 'multiformats/hashes/sha2';

export class ipfsParquetToCarJs {
	constructor(resources, metadata) {
		this.resources = resources;
		this.metadata = metadata;
		this.car_archive = null;
		this.parquet_archive = null;
		this.cids = [];
		this.bytes = [];
		this.hash = [];
	}

	// Recursive function to infer Parquet schema from nested objects
	inferParquetSchema(obj) {
		let schemaFields = {};
		for (let key in obj) {
			let value = obj[key];
            let value_type = typeof value;
			if (value_type === 'string') {
				schemaFields[key] = { type: 'UTF8' };
			} else if (value_type === 'number') {
				schemaFields[key] = {
					type: Number.isInteger(value) ? 'INT64' : 'DOUBLE',
				};
			} else if (value_type === 'boolean') {
				schemaFields[key] = { type: 'BOOLEAN' };
			} else if (value_type === 'bigint') {
				schemaFields[key] = { type: 'INT64' };
			} else if (Array.isArray(value)) {
				// Handle arrays (lists)
				if (value.length > 0) {
					let firstElement = value[0];
					if (typeof firstElement === 'object' || Array.isArray(firstElement)) {
						// Array of objects or arrays
						schemaFields[key] = {
                            repeated: true,
							fields: this.inferParquetSchema(firstElement),
						};
					} else {
						// Array of primitives
						let elementType;
						if (typeof firstElement === 'string') {
							elementType = 'UTF8';
						} else if (typeof firstElement === 'number') {
							elementType = Number.isInteger(firstElement) ? 'INT64' : 'DOUBLE';
						} else if (typeof firstElement === 'boolean') {
							elementType = 'BOOLEAN';
						} else {
							elementType = 'UTF8'; // Fallback
						}
						schemaFields[key] = {
                            repeated: true,
							type: elementType,
						};
					}
				} else {
					// Empty array, default to repeated UTF8
					schemaFields[key] = { type: 'UTF8', repeated: true };
				}
			} else if (typeof value === 'object' && value !== null) {
				// Nested object
				schemaFields[key] = {
					fields: this.inferParquetSchema(value),
				};
			} else {
				// Fallback for unsupported types
				schemaFields[key] = { type: 'UTF8' };
			}
		}
		return schemaFields;
	}

	// Adjusted denormalizeRecordForParquet function
	denormalizeRecordForParquet(record) {
		if (Array.isArray(record)) {
			// Return the array as is, processing each item recursively
			return {
                list: record.map((item) => ({
                    element: this.denormalizeRecordForParquet(item),
                }))
            };
		} else if (typeof record === 'object' && record !== null) {
			// Process each key in the object
			let denormalized = {};
			for (let key in record) {
				denormalized[key] = this.denormalizeRecordForParquet(record[key]);
			}
			return denormalized;
		} else {
			// Primitive value
			return record;
		}
	}

	denormalizeRecordForParquetSchema(record) {
        let record_type = typeof record;
		if (Array.isArray(record)) {
			// Return the array as is, processing each item recursively
			return  record.map((item) => this.denormalizeRecordForParquetSchema(item));
		} else if (typeof record === 'object' && record !== null) {
			// Process each key in the object
			let denormalized = {};
			for (let key in record) {
				denormalized[key] = this.denormalizeRecordForParquetSchema(record[key]);
			}
			return denormalized;
		} else {
			// Primitive value
			return record;
		}
	}

	// Adjusted normalizeRecordAfterParquet function
	normalizeRecordAfterParquet(record) {
		if (Array.isArray(record)) {
			// Process each item in the array
			return record.map((item) => this.normalizeRecordAfterParquet(item));
		} else if (typeof record === 'object' && record !== null) {
			// Process each key in the object
			let normalized = {};
			for (let key in record) {
                if (key === 'list') {
					normalized = record[key].map((item) => {
						let element = item.element;
                        let elementType = typeof element;
						if (elementType === 'bigint'){
                            element = element.toString();
                            element = element.replace('n', '');
                            element = parseInt(element);                               
						}
						return this.normalizeRecordAfterParquet(element);
					});
				}
                else{
                    normalized[key] = this.normalizeRecordAfterParquet(record[key]);
                }
			}
			return normalized;
		} else {
			// Primitive value
			return record;
		}
    }

	async convert_parquet_to_car(parquet_file, dst_path = null) {
		try {
			const this_dir = path.dirname(import.meta.url).replace('file://', '');
			let parquet_path = parquet_file;
			if (!parquet_path.startsWith('/')) {
				parquet_path = path.join(this_dir, parquet_path);
			}
			if (!dst_path) {
				dst_path = parquet_path.replace('.parquet', '.car');
			} else if (!dst_path.startsWith('/')) {
				dst_path = path.join(this_dir, dst_path);
			}
			const parquet = await parquetjs.ParquetReader.openFile(parquet_path);
            const schema = parquet.getSchema();
			const cursor = parquet.getCursor();
			let record = null;
			while ((record = await cursor.next())) {
				// Normalize the record after reading from Parquet
				let normalizedRecord = this.normalizeRecordAfterParquet(record);

				// Encode the record using dag-cbor
				const block = await Block.encode({
					value: normalizedRecord,
					codec: codec,
					hasher: sha256,
				});
				this.bytes.push(block.bytes);
				this.cids.push(block.cid);
			}
			// Create the CAR file
			let { writer, out } = await CarWriter.create([this.cids[0]]);
			if (dst_path) {
				Readable.from(out).pipe(fs.createWriteStream(dst_path));
			} else {
				Readable.from(out).pipe(
					fs.createWriteStream(parquet_path.replace('.parquet', '.car'))
				);
			}
			const len_cids = this.cids.length;
			for (let i = 0; i < len_cids; i++) {
				let this_cid = this.cids[i];
				let this_bytes = this.bytes[i];
				await writer.put({ cid: this_cid, bytes: this_bytes });
			}
			writer.close();
            return true
		} catch (e) {
			console.log(e);
            return false;
		}
	}


	async convert_car_to_parquet(car_file, dst_path = null) {
		try {
			let car_path = car_file;
			const this_dir = path.dirname(import.meta.url).replace('file://', '');
			if (!car_path.startsWith('/')) {
				car_path = path.join(this_dir, car_path);
			}
			if (!dst_path) {
				dst_path = car_path.replace('.car', '.parquet');
			} else if (!dst_path.startsWith('/')) {
				dst_path = path.join(this_dir, dst_path);
			}
			const inStream = fs.createReadStream(car_path);
			const reader = await CarReader.fromIterable(inStream);
			let records = [];

			// Iterate over all blocks
            let schemaRecord = null
			for await (const { cid, bytes } of reader.blocks()) {
				const block = await Block.decode({
					bytes,
					codec: codec,
					hasher: sha256,
				});
				let record = block.value;
                if (schemaRecord === null){
                    schemaRecord = this.denormalizeRecordForParquet(record);
                }
				// Denormalize the record for Parquet
				let denormalizedRecord = this.denormalizeRecordForParquet(record);

				records.push(denormalizedRecord);
			}

			// Infer schema from records using the recursive function
			let schemaFields = {};
			if (records.length > 0) {
				let sampleRecord = records[0];
				schemaFields = this.inferParquetSchema(schemaRecord);
			}

			// Print schema for debugging
			// console.log('Inferred Parquet Schema:', JSON.stringify(schemaFields, null, 2));

			const schema = new parquetjs.ParquetSchema(schemaFields);
			const writer = await parquetjs.ParquetWriter.openFile(schema, dst_path);
			for (let record of records) {
				await writer.appendRow(record);
			}
			await writer.close();
            return true;
		} catch (e) {
			console.log(e);
            return false;
		}
	}

	async test() {
		let parquet_file =
			'bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet';
		let car_file =
			'bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.car';

        let results = {};
		try {
            results['parquet_to_car'] = await this.convert_parquet_to_car(parquet_file, 'example.car');
		} catch (e) {
            results['parquet_to_car'] = e;
			console.log(e);
		}

        try {
            results['car_to_parquet'] = await this.convert_car_to_parquet(car_file, 'example.parquet');
        } catch (e) {
            results['car_to_parquet'] = e;
            console.log(e);
        }
        return results;
	}
}
export default ipfsParquetToCarJs;