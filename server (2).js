'use strict';

const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 2000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'appdb';

// Global counters for database operations
const dbOpCounters = {
  reads: 0,
  writes: 0,
  updates: 0,
  inserts: 0,
  deletes: 0,
  queries: 0,
  aggregations: 0
};

// In-memory cache for frequently accessed data
const cache = {
  latestSlots: new Map(), // Cache latest slot info
  userStatus: new Map(),  // Cache user status temporarily
  maxIndexes: new Map(),  // Cache max indexes per collection
  ttl: 30000 // 30 seconds TTL
};

// Function to log database operations with detailed info
function logDbOperation(operation, collection, query = {}, result = null, executionTime = 0) {
  const timestamp = new Date().toISOString();
  const queryStr = JSON.stringify(query).length > 100 ? 
    JSON.stringify(query).substring(0, 100) + '...' : 
    JSON.stringify(query);
  
  console.log(`[DB-${operation.toUpperCase()}] ${timestamp} | Collection: ${collection} | Query: ${queryStr} | Time: ${executionTime}ms`);
  
  if (result && typeof result === 'object') {
    if (result.matchedCount !== undefined) {
      console.log(`[DB-RESULT] Matched: ${result.matchedCount}, Modified: ${result.modifiedCount}, Upserted: ${result.upsertedCount || 0}`);
    } else if (Array.isArray(result)) {
      console.log(`[DB-RESULT] Documents returned: ${result.length}`);
    } else if (result._id) {
      console.log(`[DB-RESULT] Document ID: ${result._id}`);
    }
  }
  
  // Update counters
  switch (operation.toLowerCase()) {
    case 'find':
    case 'findone':
    case 'count':
      dbOpCounters.reads++;
      dbOpCounters.queries++;
      break;
    case 'aggregate':
      dbOpCounters.reads++;
      dbOpCounters.aggregations++;
      break;
    case 'insertone':
    case 'insertmany':
      dbOpCounters.writes++;
      dbOpCounters.inserts++;
      break;
    case 'updateone':
    case 'updatemany':
    case 'findoneandupdate':
      dbOpCounters.writes++;
      dbOpCounters.updates++;
      break;
    case 'deleteone':
    case 'deletemany':
      dbOpCounters.writes++;
      dbOpCounters.deletes++;
      break;
    case 'bulkwrite':
      dbOpCounters.writes++;
      dbOpCounters.updates++;
      break;
  }
  
  console.log(`[DB-COUNTERS] Total - Reads: ${dbOpCounters.reads}, Writes: ${dbOpCounters.writes}, Queries: ${dbOpCounters.queries}`);
}

// Cache helper functions
function setCacheValue(key, value, ttl = cache.ttl) {
  cache[key] = cache[key] || new Map();
  cache[key].set('value', value);
  cache[key].set('timestamp', Date.now());
  cache[key].set('ttl', ttl);
}

function getCacheValue(key) {
  if (!cache[key]) return null;
  
  const timestamp = cache[key].get('timestamp');
  const ttl = cache[key].get('ttl');
  const value = cache[key].get('value');
  
  if (Date.now() - timestamp > ttl) {
    cache[key].clear();
    return null;
  }
  
  return value;
}

// Endpoint to get database operation statistics
app.get('/api/db-stats', (req, res) => {
  const uptime = process.uptime();
  const stats = {
    ...dbOpCounters,
    uptime: uptime,
    operationsPerSecond: {
      reads: (dbOpCounters.reads / uptime).toFixed(2),
      writes: (dbOpCounters.writes / uptime).toFixed(2),
      total: ((dbOpCounters.reads + dbOpCounters.writes) / uptime).toFixed(2)
    },
    timestamp: new Date().toISOString(),
    cacheStats: {
      latestSlots: cache.latestSlots.size,
      userStatus: cache.userStatus.size,
      maxIndexes: cache.maxIndexes.size
    }
  };
  
  console.log('[DB-STATS-REQUEST] Database statistics requested');
  res.json(stats);
});

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use((req, res, next) => {
  console.log(`[HTTP] ${new Date().toISOString()} ${req.method} ${req.originalUrl}`);
  next();
});
app.use('/api', rateLimit({
  windowMs: 1000,
  max: 1000,
  standardHeaders: true,
  legacyHeaders: false,
}));

let client, db;

app.get('/api/feed/:contentType/:userId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { contentType, userId } = req.params;

    if (!['posts', 'reels'].includes(contentType)) {
      return res.status(400).json({ error: 'Invalid content type' });
    }

    console.log(`[FEED-REQUEST] User: ${userId}, Content: ${contentType}`);

    const feedData = await dbManager.getOptimizedFeed(userId, contentType);

    const executionTime = Date.now() - startTime;
    console.log(`[FEED-RESPONSE] User: ${userId}, Items: ${feedData.content.length}, New: ${feedData.isNewUser}, HasNew: ${feedData.hasNewContent || false}, Time: ${executionTime}ms`);

    return res.json({
      success: true,
      ...feedData
    });

  } catch (e) {
    console.error('[FEED-ERROR]', e);
    return res.status(500).json({ error: 'Failed to load feed' });
  }
});

app.post('/api/contributed-views/batch-optimized', async (req, res) => {
  const startTime = Date.now();
  try {
    const { userId, posts, reels } = req.body;

    if (!userId) {
      return res.status(400).json({ error: 'userId required' });
    }

    const postsArray = Array.isArray(posts) ? posts : [];
    const reelsArray = Array.isArray(reels) ? reels : [];

    if (postsArray.length === 0 && reelsArray.length === 0) {
      return res.json({ success: true, message: 'No content to process' });
    }

    console.log(`[CONTRIB-BATCH] User: ${userId}, Posts: ${postsArray.length}, Reels: ${reelsArray.length}`);

    const results = await dbManager.batchPutContributedViewsOptimized(userId, postsArray, reelsArray);

    const executionTime = Date.now() - startTime;
    console.log(`[CONTRIB-BATCH-COMPLETE] User: ${userId}, Time: ${executionTime}ms`);

    return res.json({
      success: true,
      message: 'Contributed views processed successfully',
      processed: {
        posts: postsArray.length,
        reels: reelsArray.length
      },
      results: results.map(r => ({
        type: r.type,
        matched: r.result.matchedCount || 0,
        modified: r.result.modifiedCount || 0,
        upserted: r.result.upsertedCount || 0
      }))
    });

  } catch (e) {
    console.error('[CONTRIB-BATCH-ERROR]', e);
    return res.status(500).json({ error: 'Failed to process contributed views' });
  }
});

async function initMongo() {
  console.log('[MONGO-INIT] Starting MongoDB connection...');
  client = new MongoClient(MONGODB_URI, {
    maxPoolSize: parseInt(process.env.MONGO_POOL_SIZE || '50', 10),
    serverSelectionTimeoutMS: 5000,
  });
  
  await client.connect();
  db = client.db(DB_NAME);
  console.log(`[MONGO-INIT] Connected to database: ${DB_NAME}`);

  // Ensure indexes idempotently (no data clearing)
  const ensureIndex = async (collName, spec, opts = {}) => {
    try {
      const startTime = Date.now();
      const coll = db.collection(collName);
      
      // Check if index exists without logging as DB operation
      const existing = await coll.indexes().catch(() => []);
      
      const found = existing.some(i => JSON.stringify(i.key) === JSON.stringify(spec));
      if (!found) {
        const createTime = Date.now();
        await coll.createIndex(spec, opts);
        const executionTime = Date.now() - createTime;
        logDbOperation('createIndex', collName, spec, { created: true }, executionTime);
        console.log(`[INDEX-CREATED] ${JSON.stringify(spec)} on ${collName} in ${executionTime}ms`);
      } else {
        console.log(`[INDEX-EXISTS] ${JSON.stringify(spec)} on ${collName}`);
      }
    } catch (e) {
      console.warn(`[INDEX-ERROR] ${collName}:`, e.message || e);
    }
  };

  console.log('[MONGO-INIT] Ensuring indexes...');
  await Promise.all([
    ensureIndex('posts', { index: 1 }),
    ensureIndex('reels', { index: 1 }),
    ensureIndex('user_posts', { userId: 1, postId: 1 }, { unique: true, sparse: true }),
    ensureIndex('user_status', { _id: 1 }, { unique: true }),
    ensureIndex('contrib_posts', { userId: 1 }),
    ensureIndex('contrib_reels', { userId: 1 }),
  ]);

  console.log('[MONGO-INIT] All indexes ensured successfully');
}

class DatabaseManager {
  constructor(db) { 
    this.db = db;
  }

  async getUserStatus(userId) {
    // Check cache first
    const cacheKey = `user_status_${userId}`;
    const cached = getCacheValue(cacheKey);
    if (cached) {
      console.log(`[USER-STATUS-CACHE] User: ${userId}, Cache hit`);
      return cached;
    }

    const startTime = Date.now();
    const query = { _id: userId };
    
    const statusDoc = await this.db.collection('user_status').findOne(query);
    const executionTime = Date.now() - startTime;
    
    logDbOperation('findOne', 'user_status', query, statusDoc, executionTime);
    console.log(`[USER-STATUS] User: ${userId}, Found: ${!!statusDoc}`);
    
    // Cache the result for 30 seconds
    if (statusDoc) {
      setCacheValue(cacheKey, statusDoc);
    }
    
    return statusDoc || null;
  }

  async updateUserStatus(userId, updates) {
    const startTime = Date.now();
    const query = { _id: userId };
    const updateDoc = {
      $set: {
        ...updates,
        updatedAt: new Date().toISOString()
      }
    };
    
    console.log(`[USER-STATUS-UPDATE] User: ${userId}, Updates: ${JSON.stringify(updates)}`);
    
    const result = await this.db.collection('user_status').updateOne(
      query,
      updateDoc,
      { upsert: true }
    );
    
    const executionTime = Date.now() - startTime;
    logDbOperation('updateOne', 'user_status', query, result, executionTime);

    // Update cache
    const cacheKey = `user_status_${userId}`;
    const updatedDoc = { _id: userId, ...updates, updatedAt: new Date().toISOString() };
    setCacheValue(cacheKey, updatedDoc);
  }

  async getOptimizedFeed(userId, contentType, minContentRequired = 6) {
    const startTime = Date.now();
    console.log(`[FEED-START] User: ${userId}, Type: ${contentType}`);
    
    const isReel = contentType === 'reels';
    const collection = isReel ? 'reels' : 'posts';
    const statusField = isReel ? 'latestReelSlotId' : 'latestPostSlotId';
    const normalField = isReel ? 'normalReelSlotId' : 'normalPostSlotId';

    // 1. Single O(1) status lookup with caching
    const userStatus = await this.getUserStatus(userId);
    const isNewUser = !userStatus || !userStatus[statusField];

    let result;
    if (isNewUser) {
      console.log(`[FEED-PATH] New user path for ${userId}`);
      result = await this.getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired);
    } else {
      console.log(`[FEED-PATH] Returning user path for ${userId}`);
      result = await this.getOptimizedFeedForReturningUser(userId, collection, contentType, userStatus, statusField, normalField, minContentRequired);
    }

    const totalTime = Date.now() - startTime;
    console.log(`[FEED-COMPLETE] User: ${userId}, Time: ${totalTime}ms, Items: ${result.content.length}`);
    
    return result;
  }

  async getLatestSlotOptimized(collection) {
    // Check cache first
    const cacheKey = `latest_${collection}`;
    const cached = getCacheValue(cacheKey);
    if (cached) {
      console.log(`[LATEST-SLOT-CACHE] Collection: ${collection}, Cache hit`);
      return cached;
    }

    const startTime = Date.now();
    const latestQuery = {};
    const latestProjection = { _id: 1, index: 1, count: 1 };
    
    const latestSlot = await this.db.collection(collection)
      .findOne(latestQuery, {
        sort: { index: -1 },
        projection: latestProjection
      });
    
    const latestTime = Date.now() - startTime;
    logDbOperation('findOne', collection, { query: latestQuery, projection: latestProjection }, latestSlot, latestTime);

    // Cache for 10 seconds (shorter TTL for frequently changing data)
    if (latestSlot) {
      setCacheValue(cacheKey, latestSlot, 10000);
    }

    return latestSlot;
  }

  async getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired) {
    console.log(`[NEW-USER-FEED] Starting for user: ${userId}, collection: ${collection}`);
    
    // Get latest slot with caching
    const latestSlot = await this.getLatestSlotOptimized(collection);

    if (!latestSlot) {
      console.log(`[NEW-USER-FEED] No slots found in ${collection}`);
      return { content: [], latestDocumentId: null, normalDocumentId: null, isNewUser: true };
    }

    console.log(`[NEW-USER-FEED] Latest slot: ${latestSlot._id}, count: ${latestSlot.count}`);

    const content = [];
    const slots = [latestSlot._id];

    // If latest slot is not full, get previous slot too
    if (latestSlot.count < 2) {
      const prevStartTime = Date.now();
      const prevQuery = { index: latestSlot.index - 1 };
      const prevProjection = { _id: 1, index: 1 };
      
      const prevSlot = await this.db.collection(collection)
        .findOne(prevQuery, { projection: prevProjection });
      
      const prevTime = Date.now() - prevStartTime;
      logDbOperation('findOne', collection, { query: prevQuery, projection: prevProjection }, prevSlot, prevTime);
      
      if (prevSlot) {
        slots.unshift(prevSlot._id);
        console.log(`[NEW-USER-FEED] Added previous slot: ${prevSlot._id}`);
      }
    }

    // Fetch content from identified slots in batch
    await this.fetchContentFromSlots(slots, collection, contentType, content);

    // Update user status
    const normalDocId = slots.length > 1 ? slots[0] : latestSlot._id;
    await this.updateUserStatus(userId, {
      [statusField]: latestSlot._id,
      [normalField]: normalDocId
    });

    console.log(`[NEW-USER-FEED] Complete. Content items: ${content.length}`);
    return {
      content: content.slice(0, minContentRequired),
      latestDocumentId: latestSlot._id,
      normalDocumentId: normalDocId,
      isNewUser: true
    };
  }

  async fetchContentFromSlots(slots, collection, contentType, content) {
    // Batch fetch all slots in one query instead of multiple queries
    if (slots.length === 0) return;

    const startTime = Date.now();
    const contentQuery = { _id: { $in: slots } };
    const listKey = contentType === 'reels' ? 'reelsList' : 'postList';
    const contentProjection = { [listKey]: 1, _id: 1 };

    const slotDocs = await this.db.collection(collection)
      .find(contentQuery, { projection: contentProjection })
      .toArray();

    const executionTime = Date.now() - startTime;
    logDbOperation('find', collection, { query: contentQuery, projection: contentProjection }, slotDocs, executionTime);

    // Process results maintaining slot order
    const slotMap = new Map();
    slotDocs.forEach(doc => {
      slotMap.set(doc._id, doc[listKey] || []);
    });

    // Add content in the original slots order
    slots.forEach(slotId => {
      const items = slotMap.get(slotId) || [];
      content.push(...items);
      console.log(`[BATCH-CONTENT] Slot ${slotId}: ${items.length} items`);
    });
  }

  async getOptimizedFeedForReturningUser(userId, collection, contentType, userStatus, statusField, normalField, minContentRequired) {
    console.log(`[RETURNING-USER-FEED] Starting for user: ${userId}`);
    
    const currentLatest = userStatus[statusField];
    const currentNormal = userStatus[normalField];

    console.log(`[RETURNING-USER-FEED] Current latest: ${currentLatest}, normal: ${currentNormal}`);

    // Extract index from current latest (e.g., "posts_12" -> 12)
    const match = currentLatest.match(/_(\d+)$/);
    if (!match) {
      console.log(`[RETURNING-USER-FEED] Cannot parse ID ${currentLatest}, falling back to new user flow`);
      return await this.getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired);
    }

    const currentIndex = parseInt(match[1]);
    const nextSlotId = `${collection.slice(0, -1)}_${currentIndex + 1}`;

    console.log(`[RETURNING-USER-FEED] Looking for next slot: ${nextSlotId}`);

    // Try to get next slot (probe for new content)
    const nextStartTime = Date.now();
    const nextQuery = { _id: nextSlotId };
    const nextProjection = { _id: 1, count: 1, [contentType === 'reels' ? 'reelsList' : 'postList']: 1 };
    
    const nextSlot = await this.db.collection(collection).findOne(
      nextQuery,
      { projection: nextProjection }
    );

    const nextTime = Date.now() - nextStartTime;
    logDbOperation('findOne', collection, { query: nextQuery, projection: nextProjection }, nextSlot, nextTime);

    if (nextSlot) {
      console.log(`[RETURNING-USER-FEED] Found new content in slot: ${nextSlotId}`);
      
      const items = nextSlot[contentType === 'reels' ? 'reelsList' : 'postList'] || [];

      // Update status to new latest
      await this.updateUserStatus(userId, {
        [statusField]: nextSlotId,
        [normalField]: currentNormal
      });

      console.log(`[RETURNING-USER-FEED] New content: ${items.length} items`);
      return {
        content: items,
        latestDocumentId: nextSlotId,
        normalDocumentId: currentNormal,
        isNewUser: false,
        hasNewContent: true
      };
    }

    console.log(`[RETURNING-USER-FEED] No new content, filtering existing`);
    return await this.getFilteredContentForReturningUser(userId, collection, contentType, currentLatest, currentNormal, minContentRequired);
  }

  async getFilteredContentForReturningUser(userId, collection, contentType, latestSlotId, normalSlotId, minContentRequired) {
    console.log(`[FILTERED-CONTENT] User: ${userId}, Latest: ${latestSlotId}, Normal: ${normalSlotId}`);
    
    // Get viewed content for user
    const contribStartTime = Date.now();
    const contribQuery = { userId };
    const contribProjection = { ids: 1 };
    
    const contributedDoc = await this.db.collection(`contrib_${collection}`).findOne(
      contribQuery,
      { projection: contribProjection }
    );

    const contribTime = Date.now() - contribStartTime;
    logDbOperation('findOne', `contrib_${collection}`, { query: contribQuery, projection: contribProjection }, contributedDoc, contribTime);

    const viewedIds = new Set();
    if (contributedDoc && contributedDoc.ids) {
      contributedDoc.ids.forEach(id => viewedIds.add(id));
      console.log(`[FILTERED-CONTENT] Found ${contributedDoc.ids.length} viewed items for user ${userId}`);
    }

    // Get content from current slots and filter
    const slotsToCheck = [latestSlotId];
    if (normalSlotId && normalSlotId !== latestSlotId) {
      slotsToCheck.push(normalSlotId);
    }

    console.log(`[FILTERED-CONTENT] Checking slots: ${slotsToCheck.join(', ')}`);

    const content = [];
    
    // Use batch fetch instead of individual queries
    await this.fetchContentFromSlots(slotsToCheck, collection, contentType, content);

    // Filter out viewed content
    const filteredContent = [];
    for (const item of content) {
      if (!viewedIds.has(item.postId) && filteredContent.length < minContentRequired) {
        filteredContent.push(item);
      }
    }

    console.log(`[FILTERED-CONTENT] Final result: ${filteredContent.length} items`);
    return {
      content: filteredContent,
      latestDocumentId: latestSlotId,
      normalDocumentId: normalSlotId,
      isNewUser: false,
      hasNewContent: false
    };
  }

  async batchPutContributedViewsOptimized(userId, posts = [], reels = []) {
    console.log(`[BATCH-CONTRIB] User: ${userId}, Posts: ${posts.length}, Reels: ${reels.length}`);
    
    const results = [];
    const operations = [];

    if (posts.length > 0) {
      operations.push({
        type: 'posts',
        collection: 'contrib_posts',
        operation: {
          updateOne: {
            filter: { userId },
            update: {
              $addToSet: { ids: { $each: posts } },
              $setOnInsert: { userId, createdAt: new Date().toISOString() },
              $set: { updatedAt: new Date().toISOString() }
            },
            upsert: true
          }
        }
      });
    }

    if (reels.length > 0) {
      operations.push({
        type: 'reels',
        collection: 'contrib_reels',
        operation: {
          updateOne: {
            filter: { userId },
            update: {
              $addToSet: { ids: { $each: reels } },
              $setOnInsert: { userId, createdAt: new Date().toISOString() },
              $set: { updatedAt: new Date().toISOString() }
            },
            upsert: true
          }
        }
      });
    }

    // Execute all operations
    for (const op of operations) {
      const startTime = Date.now();
      const result = await this.db.collection(op.collection).bulkWrite([op.operation], { ordered: false });
      const executionTime = Date.now() - startTime;
      
      logDbOperation('bulkWrite', op.collection, op.operation, result, executionTime);
      results.push({ type: op.type, result });
      
      console.log(`[BATCH-CONTRIB] ${op.type} processed: ${op.type === 'posts' ? posts.length : reels.length} items in ${executionTime}ms`);
    }

    return results;
  }

  async getDocument(col, id) {
    const startTime = Date.now();
    const query = { _id: id };
    
    const result = await this.db.collection(col).findOne(query);
    const executionTime = Date.now() - startTime;
    
    logDbOperation('findOne', col, query, result, executionTime);
    console.log(`[GET-DOCUMENT] Collection: ${col}, ID: ${id}, Found: ${!!result}`);
    
    return result;
  }

  async saveDocument(col, id, data) {
    const startTime = Date.now();
    const query = { _id: id };
    const update = { $set: data };
    
    console.log(`[SAVE-DOCUMENT] Collection: ${col}, ID: ${id}`);
    
    const result = await this.db.collection(col).updateOne(query, update, { upsert: true });
    const executionTime = Date.now() - startTime;
    
    logDbOperation('updateOne', col, { query, update }, result, executionTime);
  }

  async getMaxIndexCached(collection) {
    // Check cache first
    const cacheKey = `max_index_${collection}`;
    const cached = getCacheValue(cacheKey);
    if (cached !== null) {
      console.log(`[MAX-INDEX-CACHE] Collection: ${collection}, Cache hit: ${cached}`);
      return cached;
    }

    const startTime = Date.now();
    const maxDoc = await this.db.collection(collection).find().sort({ index: -1 }).limit(1).next();
    const executionTime = Date.now() - startTime;
    
    logDbOperation('find', collection, { sort: { index: -1 }, limit: 1 }, maxDoc, executionTime);

    const maxIndex = maxDoc?.index || 0;
    
    // Cache for 5 seconds (short TTL as this changes frequently)
    setCacheValue(cacheKey, maxIndex, 5000);
    
    return maxIndex;
  }

  async allocateSlot(col, postData, maxAttempts = 5) {
    const coll = this.db.collection(col);
    const listKey = col === 'reels' ? 'reelsList' : 'postList';

    console.log(`[ALLOCATE-SLOT] Collection: ${col}, Post: ${postData.postId}, Max attempts: ${maxAttempts}`);

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        console.log(`[ALLOCATE-SLOT] Attempt ${attempt}/${maxAttempts}`);
        
        // FIRST: Try to find and update existing documents with available slots
        const updateStartTime = Date.now();
        const updateQuery = { count: { $lt: 2 } };
        const updateOperation = {
          $push: { [listKey]: postData },
          $inc: { count: 1 }
        };
        
        const updated = await coll.findOneAndUpdate(
          updateQuery,
          updateOperation,
          {
            sort: { index: -1 },
            returnOriginal: false
          }
        );

        const updateTime = Date.now() - updateStartTime;
        logDbOperation('findOneAndUpdate', col, { query: updateQuery, update: updateOperation }, updated, updateTime);

        if (updated) {
          console.log(`[ALLOCATE-SLOT] Updated existing slot: ${updated._id}, now has ${updated.count} items`);
          
          // Invalidate cache since we added content
          const cacheKey = `latest_${col}`;
          if (cache[cacheKey]) {
            cache[cacheKey].clear();
          }
          
          return updated;
        }

        console.log(`[ALLOCATE-SLOT] No available slots, creating new document`);

        // SECOND: Use cached max index to avoid duplicate read
        const maxIndex = await this.getMaxIndexCached(col);
        const nextIndex = maxIndex + 1;
        const newId = `${col}_${nextIndex}`;

        const newDoc = {
          _id: newId,
          index: nextIndex,
          count: 1,
          [listKey]: [postData]
        };

        const insertStartTime = Date.now();
        await coll.insertOne(newDoc);
        const insertTime = Date.now() - insertStartTime;
        
        logDbOperation('insertOne', col, newDoc, { insertedId: newId }, insertTime);
        console.log(`[ALLOCATE-SLOT] Created new slot: ${newId}`);
        
        // Update cache with new max index
        const maxCacheKey = `max_index_${col}`;
        setCacheValue(maxCacheKey, nextIndex, 5000);
        
        // Invalidate latest cache since we have a new latest slot
        const latestCacheKey = `latest_${col}`;
        if (cache[latestCacheKey]) {
          cache[latestCacheKey].clear();
        }
        
        return newDoc;

      } catch (error) {
        if (error.code === 11000) {
          console.log(`[ALLOCATE-SLOT] Duplicate key on attempt ${attempt}, retrying...`);
          continue;
        }
        console.error(`[ALLOCATE-SLOT-ERROR] Attempt ${attempt}:`, error);
        throw error;
      }
    }

    throw new Error('Could not allocate slot after multiple attempts');
  }

  async saveToUserPosts(userId, postData) {
    console.log(`[SAVE-USER-POST] User: ${userId}, Post: ${postData.postId}`);
    
    const startTime = Date.now();
    const coll = this.db.collection('user_posts');
    const query = { userId, postId: postData.postId };
    const update = { 
      $set: { 
        ...postData, 
        userId, 
        postId: postData.postId, 
        createdAt: new Date().toISOString() 
      } 
    };
    
    const result = await coll.updateOne(query, update, { upsert: true });
    const executionTime = Date.now() - startTime;
    
    logDbOperation('updateOne', 'user_posts', { query, update }, result, executionTime);
    
    await this.atomicIncrement(`user_post_count:${userId}`, 1);
  }

  async atomicIncrement(key, by = 1) {
    console.log(`[ATOMIC-INCREMENT] Key: ${key}, By: ${by}`);
    
    const startTime = Date.now();
    const coll = this.db.collection('counters');
    const query = { _id: key };
    const update = { $inc: { value: by } };
    
    const res = await coll.findOneAndUpdate(query, update, { upsert: true, returnDocument: 'after' });
    const executionTime = Date.now() - startTime;
    
    logDbOperation('findOneAndUpdate', 'counters', { query, update }, res, executionTime);
    
    return res.value ? res.value.value : null;
  }

  async getContributedViewsForUserType(userId, type) {
    console.log(`[GET-CONTRIB-VIEWS] User: ${userId}, Type: ${type}`);
    
    const startTime = Date.now();
    const coll = type === 'posts' ? this.db.collection('contrib_posts') : this.db.collection('contrib_reels');
    const query = { userId };
    
    const out = {};
    const cursor = coll.find(query);
    const results = [];
    
    await cursor.forEach(v => {
      results.push(v);
      if (v && Array.isArray(v.ids)) {
        out[v.session || ''] = v.ids;
      }
    });
    
    const executionTime = Date.now() - startTime;
    logDbOperation('find', `contrib_${type}`, query, results, executionTime);
    
    console.log(`[GET-CONTRIB-VIEWS] Found ${results.length} documents with ${Object.keys(out).length} sessions`);
    return out;
  }

  async getLatestDocId(col) {
    console.log(`[GET-LATEST-DOC] Collection: ${col}`);
    
    // Use cached version
    const latestSlot = await this.getLatestSlotOptimized(col);
    const latestId = latestSlot ? latestSlot._id : null;
    
    console.log(`[GET-LATEST-DOC] Collection: ${col}, Latest: ${latestId || 'none'}`);
    return latestId;
  }

  async getContributedViewsStats(userId) {
    console.log(`[GET-CONTRIB-STATS] User: ${userId}`);
    
    // Run both aggregations in parallel to reduce total execution time
    const [postsStats, reelsStats] = await Promise.all([
      this.getContribStatsForType(userId, 'contrib_posts'),
      this.getContribStatsForType(userId, 'contrib_reels')
    ]);

    const result = {
      posts: postsStats[0] || { totalSessions: 0, totalViews: 0 },
      reels: reelsStats[0] || { totalSessions: 0, totalViews: 0 }
    };
    
    console.log(`[GET-CONTRIB-STATS] User: ${userId}, Posts: ${result.posts.totalViews} views, Reels: ${result.reels.totalViews} views`);
    return result;
  }

  async getContribStatsForType(userId, collection) {
    const startTime = Date.now();
    const aggregation = [
      { $match: { userId } },
      { $group: {
        _id: null,
        totalSessions: { $sum: 1 },
        totalViews: { $sum: { $size: '$ids' } }
      }}
    ];
    
    const stats = await this.db.collection(collection).aggregate(aggregation).toArray();
    const executionTime = Date.now() - startTime;
    logDbOperation('aggregate', collection, aggregation, stats, executionTime);

    return stats;
  }
}

let dbManager;

async function start() {
  console.log('[SERVER-START] Initializing server...');
  await initMongo();
  dbManager = new DatabaseManager(db);

  console.log('[SERVER-START] MongoDB optimizations initialized - NO DATA CLEARING');
}

start().catch(err => {
  console.error('[SERVER-START-ERROR] Failed to init DB:', err);
  process.exit(1);
});

// --- ROUTES ---

app.get('/health', (req, res) => {
  console.log('[HEALTH-CHECK] Health endpoint called');
  res.json({ status: 'OK', ts: new Date().toISOString() });
});

app.get('/api/status/:userId', async (req, res) => {
  const startTime = Date.now();
  try {
    const userId = req.params.userId;
    console.log(`[STATUS-REQUEST] User: ${userId}`);
    
    // Use optimized getUserStatus which includes caching
    const status = await dbManager.getUserStatus(userId);
    
    if (status) {
      const executionTime = Date.now() - startTime;
      console.log(`[STATUS-RESPONSE] User: ${userId}, Existing user, Time: ${executionTime}ms`);
      
      return res.json({
        success: true,
        isNew: false,
        posts: {
          latest: status.latestPostSlotId,
          normal: status.normalPostSlotId
        },
        reels: {
          latest: status.latestReelSlotId,
          normal: status.normalReelSlotId
        },
        status
      });
    }
    
    console.log(`[STATUS-REQUEST] New user detected: ${userId}`);
    
    // Get both latest IDs in parallel to reduce execution time
    const [postsLatest, reelsLatest] = await Promise.all([
      dbManager.getLatestDocId('posts'),
      dbManager.getLatestDocId('reels')
    ]);
    
    const executionTime = Date.now() - startTime;
    console.log(`[STATUS-RESPONSE] User: ${userId}, New user, Time: ${executionTime}ms`);
    
    return res.json({
      success: true,
      isNew: true,
      posts: { latest: postsLatest, normal: postsLatest },
      reels: { latest: reelsLatest, normal: reelsLatest },
      computedAt: new Date().toISOString()
    });
  } catch (e) {
    console.error('[STATUS-ERROR]', e);
    return res.status(500).json({ error: 'Failed to fetch status' });
  }
});

app.get('/api/latest/:collection', async (req, res) => {
  const startTime = Date.now();
  try {
    const col = req.params.collection;
    console.log(`[LATEST-REQUEST] Collection: ${col}`);
    
    if (!['posts','reels'].includes(col)) {
      return res.status(400).json({ error: 'Invalid collection' });
    }
    
    const latest = await dbManager.getLatestDocId(col);
    const executionTime = Date.now() - startTime;
    
    console.log(`[LATEST-RESPONSE] Collection: ${col}, Latest: ${latest}, Time: ${executionTime}ms`);
    return res.json({ success: true, latest: latest || null });
  } catch (e) {
    console.error('[LATEST-ERROR]', e);
    return res.status(500).json({ error: 'Failed to fetch latest id' });
  }
});

app.get('/api/doc/:collection/:docId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { collection, docId } = req.params;
    console.log(`[DOC-REQUEST] Collection: ${collection}, ID: ${docId}`);
    
    if (!['posts','reels'].includes(collection)) {
      return res.status(400).json({ error: 'Invalid collection' });
    }
    
    const doc = await dbManager.getDocument(collection, docId);
    const executionTime = Date.now() - startTime;
    
    if (!doc) {
      console.log(`[DOC-RESPONSE] Document not found: ${docId}, Time: ${executionTime}ms`);
      return res.status(404).json({ error: 'Not found' });
    }
    
    console.log(`[DOC-RESPONSE] Document found: ${docId}, Time: ${executionTime}ms`);
    return res.json({ success: true, doc });
  } catch (e) {
    console.error('[DOC-ERROR]', e);
    return res.status(500).json({ error: 'Failed to read doc' });
  }
});

app.get('/api/contributed-views/:type/:userId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { type, userId } = req.params;
    console.log(`[CONTRIB-VIEWS-REQUEST] Type: ${type}, User: ${userId}`);
    
    if (!['posts','reels'].includes(type)) {
      return res.status(400).json({ error: 'Invalid type' });
    }
    
    const contributions = await dbManager.getContributedViewsForUserType(userId, type);
    const executionTime = Date.now() - startTime;
    
    console.log(`[CONTRIB-VIEWS-RESPONSE] Type: ${type}, User: ${userId}, Sessions: ${Object.keys(contributions).length}, Time: ${executionTime}ms`);
    return res.json({ success: true, contributions });
  } catch (e) {
    console.error('[CONTRIB-VIEWS-ERROR]', e);
    return res.status(500).json({ error: 'Failed to read contributions' });
  }
});

app.get('/api/contributed-views/stats/:userId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { userId } = req.params;
    console.log(`[CONTRIB-STATS-REQUEST] User: ${userId}`);
    
    const stats = await dbManager.getContributedViewsStats(userId);
    const executionTime = Date.now() - startTime;
    
    console.log(`[CONTRIB-STATS-RESPONSE] User: ${userId}, Time: ${executionTime}ms`);
    return res.json({ success: true, stats });
  } catch (e) {
    console.error('[CONTRIB-STATS-ERROR]', e);
    return res.status(500).json({ error: 'Failed to read contribution stats' });
  }
});

app.post('/api/posts', async (req, res) => {
  const startTime = Date.now();
  try {
    console.log('[POST-CREATE-REQUEST] New post creation request');
    console.log(`[POST-CREATE-DATA] ${JSON.stringify(req.body, null, 2)}`);
    
    const postData = req.body;
    if (!postData.userId || !postData.postId) {
      return res.status(400).json({ error: 'userId & postId required' });
    }
    
    const col = postData.isReel ? 'reels' : 'posts';
    postData.serverTimestamp = new Date().toISOString();

    console.log(`[POST-CREATE] User: ${postData.userId}, Post: ${postData.postId}, Collection: ${col}`);

    // allocate slot atomically and robustly
    const updatedDoc = await dbManager.allocateSlot(col, postData, 12);
    postData.documentID = updatedDoc._id;

    // save to user_posts
    await dbManager.saveToUserPosts(postData.userId, postData);

    const executionTime = Date.now() - startTime;
    console.log(`[POST-CREATE-SUCCESS] User: ${postData.userId}, Post: ${postData.postId}, Document: ${postData.documentID}, Time: ${executionTime}ms`);
    
    return res.json({
      success: true,
      documentID: postData.documentID,
      postId: postData.postId
    });
  } catch (e) {
    console.error('[POST-CREATE-ERROR]', e);
    return res.status(500).json({ error: (e && e.message) || 'Internal' });
  }
});

app.get('/api/stats', async (req, res) => {
  const startTime = Date.now();
  try {
    console.log('[STATS-REQUEST] System statistics requested');
    
    // Run all count operations in parallel to reduce total execution time
    const [postsCount, reelsCount, userIds, contribPostsCount, contribReelsCount] = await Promise.all([
      dbManager.getCollectionCount('posts'),
      dbManager.getCollectionCount('reels'),
      dbManager.getDistinctUserIds(),
      dbManager.getCollectionCount('contrib_posts'),
      dbManager.getCollectionCount('contrib_reels')
    ]);

    const usersCount = userIds.length;
    const executionTime = Date.now() - startTime;

    const stats = {
      reelsDocuments: reelsCount,
      postsDocuments: postsCount,
      users: usersCount,
      contributedPostsSessions: contribPostsCount,
      contributedReelsSessions: contribReelsCount,
      timestamp: new Date().toISOString(),
      executionTime: executionTime,
      operationCounts: dbOpCounters
    };

    console.log(`[STATS-RESPONSE] Posts: ${postsCount}, Reels: ${reelsCount}, Users: ${usersCount}, Time: ${executionTime}ms`);
    return res.json(stats);
  } catch (e) {
    console.error('[STATS-ERROR]', e);
    return res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// Helper methods for DatabaseManager to support optimized stats
DatabaseManager.prototype.getCollectionCount = async function(collection) {
  const startTime = Date.now();
  const count = await this.db.collection(collection).countDocuments();
  const executionTime = Date.now() - startTime;
  
  logDbOperation('countDocuments', collection, {}, { count }, executionTime);
  return count;
};

DatabaseManager.prototype.getDistinctUserIds = async function() {
  const startTime = Date.now();
  const userIds = await this.db.collection('user_posts').distinct('userId');
  const executionTime = Date.now() - startTime;
  
  logDbOperation('distinct', 'user_posts', { field: 'userId' }, { count: userIds.length }, executionTime);
  return userIds;
};

// Generic error handler
app.use((err, req, res, next) => {
  console.error('[UNHANDLED-ERROR]', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Graceful shutdown
for (const sig of ['SIGINT','SIGTERM']) {
  process.on(sig, async () => {
    console.log('[SHUTDOWN] Shutting down gracefully...');
    try {
      if (client) {
        console.log('[SHUTDOWN] Closing MongoDB connection...');
        await client.close();
        console.log('[SHUTDOWN] MongoDB connection closed');
      }
    } catch (e) {
      console.warn('[SHUTDOWN-ERROR] DB close error', e);
    }
    
    console.log(`[SHUTDOWN] Final DB Operation Counts:`);
    console.log(`[SHUTDOWN] Reads: ${dbOpCounters.reads}, Writes: ${dbOpCounters.writes}`);
    console.log(`[SHUTDOWN] Queries: ${dbOpCounters.queries}, Updates: ${dbOpCounters.updates}`);
    console.log(`[SHUTDOWN] Inserts: ${dbOpCounters.inserts}, Aggregations: ${dbOpCounters.aggregations}`);
    console.log(`[SHUTDOWN] Cache Stats - Latest: ${cache.latestSlots.size}, Status: ${cache.userStatus.size}, MaxIndex: ${cache.maxIndexes.size}`);
    
    process.exit(0);
  });
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[SERVER-LISTENING] Server listening on port ${PORT}`);
  console.log(`[SERVER-LISTENING] MongoDB URI: ${MONGODB_URI}`);
  console.log(`[SERVER-LISTENING] Database name: ${DB_NAME}`);
  console.log('[SERVER-LISTENING] Data clearing on startup: DISABLED');
  console.log('[SERVER-LISTENING] Comprehensive logging: ENABLED');
  console.log('[SERVER-LISTENING] Performance optimizations: ENABLED');
  console.log('[SERVER-LISTENING] Caching system: ENABLED');
});