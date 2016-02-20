var events = require( 'events' ),
	util = require( 'util' ),
	pckg = require( '../package.json' ),
	TingoDb = require('tingodb')().Db;

/**
 *
 * @constructor
 */
var Connector = function( options ) {
	this.isReady = false;
	this.name = pckg.name;
	this.version = pckg.version;
	this._splitChar = options.splitChar || null;
	this._defaultCollection = options.defaultCollection || 'deepstream_docs';
	this._db = null;
	this._collections = {};

	if( !options.path ) {
		throw new Error( 'Missing setting \'path\'' );
	}

	try {
		var db = new TingoDb(options.path, {});
		process.nextTick((function() {this._onConnect(null, db)}).bind(this));
	} catch (err) {
		process.nextTick((function() {this._onConnect(err)}).bind(this));
	}
};

util.inherits( Connector, events.EventEmitter );

/**
 * Writes a value to the cache.
 *
 * @param {String}   key
 * @param {Object}   value
 * @param {Function} callback Should be called with null for successful set operations or with an error message string
 *
 * @private
 * @returns {void}
 */
Connector.prototype.set = function( key, value, callback ) {
	var params = this._getParams( key );

	if( params === null ) {
		callback( 'Invalid key ' + key );
		return;
	}

	value.ds_key = params.id;
	params.collection.updateOne({ ds_key: params.id }, value, { upsert: true }, callback );
};

/**
 * Retrieves a value from the cache
 *
 * @param {String}   key
 * @param {Function} callback Will be called with null and the stored object
 *                            for successful operations or with an error message string
 *
 * @private
 * @returns {void}
 */
Connector.prototype.get = function( key, callback ) {
	var params = this._getParams( key );

	if( params === null ) {
		callback( 'Invalid key ' + key );
		return;
	}

	params.collection.findOne({ ds_key: params.id }, function( err, doc ){
		if( err ) {
			callback( err );
		} else {
			if( doc === null ) {
				callback( null, null );
			} else {
				delete doc._id;
				delete doc.ds_key;
				callback( null, doc );
			}
		}
	});
};

/**
 * Deletes an entry from the cache.
 *
 * @param   {String}   key
 * @param   {Function} callback Will be called with null for successful deletions or with
 *                     an error message string
 *
 * @private
 * @returns {void}
 */
Connector.prototype.delete = function( key, callback ) {
	var params = this._getParams( key );

	if( params === null ) {
		callback( 'Invalid key ' + key );
		return;
	}

	params.collection.deleteOne({ ds_key: params.id }, callback );
};

/**
 * Callback for established (or rejected) connections
 *
 * @param {String} error
 * @param {MongoClient} db
 *
 * @private
 * @returns {void}
 */
Connector.prototype._onConnect = function( err, db ) {
	if( err ) {
		this.emit( 'error', err );
		return;
	}

	this._db = db;
	this.isReady = true;
	this.emit( 'ready' );
};

/**
 * Determines the document id and the collection
 * to use based on the provided key
 *
 * Creates the collection if it doesn't exist yet.
 *
 * Since MongoDB ObjecIDs are adhering to a specified format
 * we'll add a new field for the key called ds_key and index the
 * collection based on it
 *
 * @param {String} key
 *
 * @private
 * @returns {Object} {connection: <MongoConnection>, id: <String> }
 */
Connector.prototype._getParams = function( key ) {
	var parts = key.split( this._splitChar ),
		collectionName,
		id;

	if( parts.length === 1 ) {
		collectionName = this._defaultCollection;
		id = key;
	}
	else if( parts.length === 2 ) {
		collectionName = parts[ 0 ];
		id = parts[ 1 ];
	}
	else {
		return null;
	}

	if( !this._collections[ collectionName ] ) {
		this._collections[ collectionName ] = this._db.collection( collectionName );
		this._collections[ collectionName ].ensureIndex({ ds_key: 1 });
	}

	return { collection: this._collections[ collectionName ], id: id };
};

module.exports = Connector;
