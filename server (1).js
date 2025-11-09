// server.js
'use strict';

const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const PORT = parseInt(process.env.PORT, 10) || 4000;
const HOST = '0.0.0.0';
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'leveldb_converted';

let client, db;
const sseClients = new Set();

// Initialize MongoDB connection
async function initMongo() {
  client = new MongoClient(MONGODB_URI); // don't use deprecated options
  await client.connect();
  db = client.db(DB_NAME);

  // Only create the index we need (don't try to re-create _id index)
  await db.collection('user_slots').createIndex({ userId: 1, slotIndex: 1 }, { unique: true });
  console.log('Connected to MongoDB at', MONGODB_URI, 'db:', DB_NAME);
}

// Broadcast real-time updates to all connected clients
function broadcastUpdate(data) {
  const message = `data: ${JSON.stringify(data)}\n\n`;
  for (const res of Array.from(sseClients)) {
    try {
      res.write(message);
    } catch (err) {
      // remove clients that error out
      try { sseClients.delete(res); } catch (e) {}
    }
  }
}

// Utility: generate next slot index for a user
async function getNextUserIndex(userId) {
  const maxDoc = await db.collection('user_slots').findOne(
    { userId },
    { sort: { slotIndex: -1 }, projection: { slotIndex: 1 } }
  );
  return maxDoc ? maxDoc.slotIndex + 1 : 1;
}

// Helper function to get all data for broadcasting
async function getAllDataForBroadcast() {
  const allSlots = await db.collection('user_slots').find({}).toArray();

  const result = {};
  let totalPosts = 0;
  let totalReels = 0;
  let totalSlots = 0;

  allSlots.forEach(slot => {
    if (!result[slot.userId]) {
      result[slot.userId] = {};
    }
    result[slot.userId][slot.slotIndex] = slot;

    totalPosts += slot.postCount || 0;
    totalReels += slot.reelCount || 0;
    totalSlots++;
  });

  const hierarchical = {
    users_posts: {},
    metadata: {
      totalUsers: Object.keys(result).length,
      totalSlots: totalSlots,
      totalPosts: totalPosts,
      totalReels: totalReels,
      lastUpdated: new Date().toISOString()
    }
  };

  for (const [userId, slots] of Object.entries(result)) {
    hierarchical.users_posts[userId] = { user_post: {} };
    for (const [slotIndex, doc] of Object.entries(slots)) {
      const docName = `${userId}_${slotIndex}`;
      hierarchical.users_posts[userId].user_post[docName] = doc;
    }
  }

  return hierarchical;
}

/* Basic endpoints */
app.get('/ping', (req, res) => res.send('pong'));

app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    database: db ? 'Connected' : 'Disconnected',
    uptime: process.uptime(),
    activeConnections: sseClients.size
  });
});

/* POST /api/posts
   Body: must contain userId. isReel optional boolean. */
app.post('/api/posts', async (req, res) => {
  try {
    const post = req.body || {};
    const { userId, isReel } = post;

    if (!userId) {
      return res.status(400).json({ error: 'userId is required' });
    }

    const slotKey = isReel ? 'reelsList' : 'postList';
    const countField = isReel ? 'reelCount' : 'postCount';

    // 1) Find first slot with space (<2)
    let targetSlot = await db.collection('user_slots').findOne(
      { userId, [countField]: { $lt: 2 } },
      { sort: { slotIndex: 1 } }
    );

    let targetIndex;
    if (!targetSlot) {
      // Create a fresh slot skeleton (will be upserted)
      targetIndex = await getNextUserIndex(userId);
      targetSlot = {
        _id: `${userId}_${targetIndex}`,
        userId,
        slotIndex: targetIndex,
        index: targetIndex,
        postCount: 0,
        reelCount: 0,
        postList: [],
        reelsList: [],
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };
      console.log(`[POST-SAVE] No available slot -> creating new slot ${targetSlot._id} for user ${userId}`);
    } else {
      targetIndex = targetSlot.slotIndex;
      console.log(`[POST-SAVE] Found existing slot ${targetSlot._id} (slotIndex=${targetIndex}) for user ${userId}`);
    }

    // 2) Build the item we will save (preserve incoming fields, add savedAt)
    const postItem = {
      ...post,
      savedAt: new Date().toISOString()
    };

    // 3) Prepare new slot contents
    const existingArray = Array.isArray(targetSlot[slotKey]) ? [...targetSlot[slotKey]] : [];
    existingArray.push(postItem);

    // 4) Make the update doc (increment the proper counter)
    const newCount = (targetSlot[countField] || 0) + 1;
    const updateDoc = {
      ...targetSlot,
      [countField]: newCount,
      [slotKey]: existingArray,
      updatedAt: new Date().toISOString()
    };

    const docId = `${userId}_${targetIndex}`;

    // 5) Log before saving (preview to keep logs readable)
    console.log('[POST-SAVE] Saving to document:', docId);
    console.log('[POST-SAVE] slotKey:', slotKey, 'newCount:', newCount);
    try {
      console.log('[POST-SAVE] postItem preview:', JSON.stringify({
        postId: postItem.postId || null,
        captionPreview: postItem.caption ? String(postItem.caption).slice(0, 120) : null,
        savedAt: postItem.savedAt
      }));
    } catch (e) {
      console.log('[POST-SAVE] Unable to stringify postItem preview', e);
    }

    // 6) Upsert the slot document
    const replaceResult = await db.collection('user_slots').replaceOne(
      { _id: docId },
      updateDoc,
      { upsert: true }
    );

    // 7) Read back saved slot for verification and robust logging
    const savedSlot = await db.collection('user_slots').findOne({ _id: docId });

    // Try to find the exact saved item inside the saved slot
    let savedItem = null;
    if (savedSlot && Array.isArray(savedSlot[slotKey])) {
      savedItem = savedSlot[slotKey].find(i => {
        // Prefer matching by postId if present, otherwise match by savedAt timestamp
        if (i.postId && postItem.postId) return i.postId === postItem.postId;
        return i.savedAt === postItem.savedAt;
      });
    }

    console.log('[POST-SAVE] replaceOne result:', {
      matchedCount: replaceResult.matchedCount,
      modifiedCount: replaceResult.modifiedCount,
      upsertedId: replaceResult.upsertedId ? replaceResult.upsertedId._id || replaceResult.upsertedId : null,
      acknowledged: replaceResult.acknowledged
    });

    console.log('[POST-SAVE] Saved slot id:', docId, 'slotIndex:', targetIndex, 'savedItemFound:', !!savedItem);

    if (savedItem) {
      try {
        console.log('[POST-SAVE] Saved item details:', JSON.stringify({
          postId: savedItem.postId || null,
          captionPreview: savedItem.caption ? String(savedItem.caption).slice(0, 200) : null,
          savedAt: savedItem.savedAt,
          viewcount: savedItem.viewcount || 0,
          likeCount: savedItem.likeCount || 0,
          isReel: !!isReel
        }, null, 2));
      } catch (e) {
        console.log('[POST-SAVE] Saved item found but failed to stringify preview', e);
      }
    } else {
      console.warn('[POST-SAVE] WARNING: Unable to locate the saved item inside document. Document snapshot (summary):',
        JSON.stringify({
          _id: savedSlot ? savedSlot._id : null,
          postCount: savedSlot ? savedSlot.postCount : null,
          reelCount: savedSlot ? savedSlot.reelCount : null,
          postListLen: savedSlot && Array.isArray(savedSlot.postList) ? savedSlot.postList.length : 0,
          reelsListLen: savedSlot && Array.isArray(savedSlot.reelsList) ? savedSlot.reelsList.length : 0
        }, null, 2));
    }

    // 8) Broadcast SSE update (best-effort)
    try {
      const allData = await getAllDataForBroadcast();
      broadcastUpdate(allData);
    } catch (e) {
      console.warn('[POST-SAVE] Broadcast update failed:', e && e.message ? e.message : e);
    }

    // 9) Return response consistent with previous API
    return res.status(200).json({
      success: true,
      slotIndex: targetIndex,
      totalItems: updateDoc[countField],
      docName: docId,
      timestamp: new Date().toISOString()
    });

  } catch (err) {
    console.error('POST /api/posts error', err && err.stack ? err.stack : err);
    return res.status(500).json({ error: 'Server error: ' + (err && err.message ? err.message : String(err)) });
  }
});
/* DELETE /api/posts/user/:userId - delete all docs for a user */
app.delete('/api/posts/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await db.collection('user_slots').deleteMany({ userId });

    // Broadcast updated data
    const allData = await getAllDataForBroadcast();
    broadcastUpdate(allData);

    return res.status(200).json({
      success: true,
      message: `All posts for user ${userId} deleted`,
      deletedCount: result.deletedCount,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('DELETE /api/posts/user/:userId error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET /api/posts/stats */
app.get('/api/posts/stats', async (req, res) => {
  try {
    const allSlots = await db.collection('user_slots').find({}).toArray();

    const stats = {
      totalUsers: 0,
      totalSlots: 0,
      totalPosts: 0,
      totalReels: 0,
      users: {},
      generatedAt: new Date().toISOString()
    };

    const userMap = {};

    allSlots.forEach(slot => {
      const userId = slot.userId;
      const slotIndex = slot.slotIndex;

      if (!userMap[userId]) {
        userMap[userId] = {
          slots: 0,
          posts: 0,
          reels: 0,
          slotDetails: {}
        };
        stats.totalUsers++;
      }

      userMap[userId].slots++;
      userMap[userId].posts += slot.postCount || 0;
      userMap[userId].reels += slot.reelCount || 0;
      userMap[userId].slotDetails[slotIndex] = {
        postCount: slot.postCount || 0,
        reelCount: slot.reelCount || 0,
        createdAt: slot.createdAt,
        updatedAt: slot.updatedAt
      };

      stats.totalSlots++;
      stats.totalPosts += slot.postCount || 0;
      stats.totalReels += slot.reelCount || 0;
    });

    stats.users = userMap;

    return res.json(stats);
  } catch (err) {
    console.error('GET /api/posts/stats error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* Server-Sent Events stream */
app.get('/api/posts/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Cache-Control'
  });
  
  
  
  
  app.delete('/api/following-views/clear', async (req, res) => {
  try {
    console.log('[FOLLOWING-VIEWS] Clearing all following views collection');

    const result = await db.collection('contributed_views_following').deleteMany({});

    console.log('[FOLLOWING-VIEWS] Clear result:');
    console.log('  Deleted count:', result.deletedCount);
    console.log('  Acknowledged:', result.acknowledged);

    // Broadcast update
    try {
      const allData = await getAllDataForBroadcast();
      broadcastUpdate(allData);
    } catch (e) {
      console.warn('[FOLLOWING-VIEWS] Broadcast update failed:', e.message);
    }

    return res.json({
      success: true,
      message: 'All ContributedViewsToFollowing documents cleared',
      deletedCount: result.deletedCount,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('[FOLLOWING-VIEWS] ERROR clearing collection:', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});
  
  
  
  app.delete('/api/following-views/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    if (!userId) {
      return res.status(400).json({ error: 'userId is required' });
    }

    console.log('[FOLLOWING-VIEWS] Deleting all following views for user:', userId);

    const result = await db.collection('contributed_views_following').deleteMany({ userId });

    console.log('[FOLLOWING-VIEWS] Deletion result:');
    console.log('  Deleted count:', result.deletedCount);
    console.log('  Acknowledged:', result.acknowledged);

    // Broadcast update
    try {
      const allData = await getAllDataForBroadcast();
      broadcastUpdate(allData);
    } catch (e) {
      console.warn('[FOLLOWING-VIEWS] Broadcast update failed:', e.message);
    }

    return res.json({
      success: true,
      message: `Deleted all following views for user ${userId}`,
      deletedCount: result.deletedCount,
      userId: userId,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('[FOLLOWING-VIEWS] ERROR deleting user following views:', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

  
  
  
  
  
  
  app.get('/api/following-views/stats', async (req, res) => {
  try {
    console.log('[FOLLOWING-VIEWS] Getting collection statistics');

    const allDocs = await db.collection('contributed_views_following').find({}).toArray();

    const stats = {
      totalUsers: 0,
      totalDocuments: allDocs.length,
      totalPosts: 0,
      totalReels: 0,
      users: {},
      generatedAt: new Date().toISOString()
    };

    const userMap = {};

    allDocs.forEach(doc => {
      const userId = doc.userId;
      const postsCount = doc.postCount || (doc.PostList ? doc.PostList.length : 0);
      const reelsCount = doc.reelCount || (doc.reelsList ? doc.reelsList.length : 0);

      if (!userMap[userId]) {
        userMap[userId] = {
          documents: 0,
          posts: 0,
          reels: 0,
          documentDetails: {}
        };
        stats.totalUsers++;
      }

      userMap[userId].documents++;
      userMap[userId].posts += postsCount;
      userMap[userId].reels += reelsCount;
      userMap[userId].documentDetails[doc.documentName] = {
        postCount: postsCount,
        reelCount: reelsCount,
        createdAt: doc.createdAt,
        updatedAt: doc.updatedAt
      };

      stats.totalPosts += postsCount;
      stats.totalReels += reelsCount;
    });

    stats.users = userMap;

    console.log('[FOLLOWING-VIEWS] Stats generated:');
    console.log('  Total users:', stats.totalUsers);
    console.log('  Total documents:', stats.totalDocuments);
    console.log('  Total posts:', stats.totalPosts);
    console.log('  Total reels:', stats.totalReels);

    return res.json(stats);
  } catch (err) {
    console.error('[FOLLOWING-VIEWS] ERROR getting stats:', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});
  
  
  
  
  app.get('/api/following-views/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    if (!userId) {
      return res.status(400).json({ error: 'userId is required' });
    }

    console.log('[FOLLOWING-VIEWS] Getting following views for user:', userId);

    const userFollowingViews = await db.collection('contributed_views_following').find({ userId }).toArray();

    let totalPosts = 0;
    let totalReels = 0;
    let totalDocuments = userFollowingViews.length;

    const processedDocs = userFollowingViews.map(doc => {
      const postsCount = doc.postCount || (doc.PostList ? doc.PostList.length : 0);
      const reelsCount = doc.reelCount || (doc.reelsList ? doc.reelsList.length : 0);
      
      totalPosts += postsCount;
      totalReels += reelsCount;

      return {
        documentName: doc.documentName,
        documentId: doc._id,
        PostList: doc.PostList || [],
        reelsList: doc.reelsList || [],
        postCount: postsCount,
        reelCount: reelsCount,
        createdAt: doc.createdAt,
        updatedAt: doc.updatedAt
      };
    });

    console.log('[FOLLOWING-VIEWS] Found', totalDocuments, 'following view documents for user');
    console.log('[FOLLOWING-VIEWS] Total posts:', totalPosts, 'Total reels:', totalReels);

    return res.json({
      success: true,
      userId: userId,
      documents: processedDocs,
      metadata: {
        totalDocuments: totalDocuments,
        totalPosts: totalPosts,
        totalReels: totalReels,
        fetchedAt: new Date().toISOString()
      }
    });
  } catch (err) {
    console.error('[FOLLOWING-VIEWS] ERROR getting user following views:', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});
  
  
  
  
  
  
  
  /* POST /api/following-views - Save ContributedViewsToFollowing data */
app.post('/api/following-views', async (req, res) => {
  try {
    console.log('[FOLLOWING-VIEWS] Received following views sync request');
    const { userId, documentName, postIds, reelIds, postCount, reelCount } = req.body;

    if (!userId || !documentName) {
      console.log('[FOLLOWING-VIEWS] ERROR: Missing required fields - userId or documentName');
      return res.status(400).json({ error: 'userId and documentName are required' });
    }

    console.log('[FOLLOWING-VIEWS] Processing following views sync:');
    console.log('  UserId:', userId);
    console.log('  DocumentName:', documentName);
    console.log('  Posts count:', postCount || 0);
    console.log('  Reels count:', reelCount || 0);

    // Create the document data structure
    const contributedViewData = {
      _id: `${userId}_${documentName}`,
      userId: userId,
      documentName: documentName,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    // Add posts data if provided
    if (postIds && Array.isArray(postIds) && postIds.length > 0) {
      contributedViewData.PostList = postIds;
      contributedViewData.postCount = postCount || postIds.length;
      console.log('[FOLLOWING-VIEWS] Added PostList with', postIds.length, 'posts');
    }

    // Add reels data if provided
    if (reelIds && Array.isArray(reelIds) && reelIds.length > 0) {
      contributedViewData.reelsList = reelIds;
      contributedViewData.reelCount = reelCount || reelIds.length;
      console.log('[FOLLOWING-VIEWS] Added reelsList with', reelIds.length, 'reels');
    }

    console.log('[FOLLOWING-VIEWS] Final data structure:');
    console.log('  Document ID:', contributedViewData._id);
    console.log('  PostList length:', contributedViewData.PostList ? contributedViewData.PostList.length : 0);
    console.log('  ReelsList length:', contributedViewData.reelsList ? contributedViewData.reelsList.length : 0);

    // Use upsert to handle both create and update scenarios
    const result = await db.collection('contributed_views_following').replaceOne(
      { _id: contributedViewData._id },
      contributedViewData,
      { upsert: true }
    );

    console.log('[FOLLOWING-VIEWS] MongoDB operation result:');
    console.log('  Matched count:', result.matchedCount);
    console.log('  Modified count:', result.modifiedCount);
    console.log('  Upserted ID:', result.upsertedId ? result.upsertedId._id || result.upsertedId : 'none');
    console.log('  Operation acknowledged:', result.acknowledged);

    // Verify the saved document
    const savedDoc = await db.collection('contributed_views_following').findOne({ _id: contributedViewData._id });
    if (savedDoc) {
      console.log('[FOLLOWING-VIEWS] Document successfully saved:');
      console.log('  Document ID:', savedDoc._id);
      console.log('  UserId:', savedDoc.userId);
      console.log('  DocumentName:', savedDoc.documentName);
      console.log('  Posts saved:', savedDoc.PostList ? savedDoc.PostList.length : 0);
      console.log('  Reels saved:', savedDoc.reelsList ? savedDoc.reelsList.length : 0);
    } else {
      console.warn('[FOLLOWING-VIEWS] WARNING: Could not verify saved document');
    }

    // Broadcast update to SSE clients (if needed for real-time updates)
    try {
      const allData = await getAllDataForBroadcast();
      broadcastUpdate(allData);
    } catch (e) {
      console.warn('[FOLLOWING-VIEWS] Broadcast update failed:', e.message);
    }

    return res.status(200).json({
      success: true,
      message: 'Following views synced successfully',
      userId: userId,
      documentName: documentName,
      documentId: contributedViewData._id,
      postsCount: contributedViewData.postCount || 0,
      reelsCount: contributedViewData.reelCount || 0,
      timestamp: new Date().toISOString()
    });

  } catch (err) {
    console.error('[FOLLOWING-VIEWS] ERROR in following views sync:', err);
    console.error('[FOLLOWING-VIEWS] Error stack:', err.stack);
    return res.status(500).json({ 
      error: 'Server error during following views sync: ' + err.message 
    });
  }
});
  
  
  
  
  
  
  
  
  

  // Add client to active connections
  sseClients.add(res);

  // Send initial data
  getAllDataForBroadcast().then(data => {
    try {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch (err) {
      sseClients.delete(res);
    }
  }).catch(err => {
    console.error('Error sending initial data:', err);
  });

  // Clean up on client disconnect
  req.on('close', () => {
    sseClients.delete(res);
  });

  req.on('error', () => {
    sseClients.delete(res);
  });
});

/* Database viewer page */
app.get('/database-viewer', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>MongoDB Clone - Database Viewer</title>
<style>
  body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial; background:#111; color:#eee; margin:0; }
  .header { padding:16px; display:flex; justify-content:space-between; align-items:center; background:linear-gradient(90deg,#1e1e2f,#2a2a3a); }
  .container { padding:16px; height: calc(100vh - 64px); overflow:auto; }
  pre { white-space:pre-wrap; background:#0b0b0b; padding:12px; border-radius:8px; }
</style>
</head>
<body>
  <div class="header"><strong>🍃 MongoDB Clone Database Viewer</strong><span id="status">connecting...</span></div>
  <div class="container"><pre id="payload">Loading...</pre></div>
  <script>
    const payloadEl = document.getElementById('payload');
    const statusEl = document.getElementById('status');

    fetch('/api/posts/all').then(r=>r.json()).then(j=>{
      payloadEl.textContent = JSON.stringify(j,null,2);
      statusEl.textContent = 'loaded';
    }).catch(e=>{
      payloadEl.textContent = 'Failed to load initial data: ' + e;
      statusEl.textContent = 'error';
    });

    const es = new EventSource('/api/posts/stream');
    es.onopen = () => statusEl.textContent = 'connected';
    es.onerror = () => statusEl.textContent = 'disconnected';
    es.onmessage = (evt) => {
      try {
        const parsed = JSON.parse(evt.data);
        payloadEl.textContent = JSON.stringify(parsed, null, 2);
      } catch (e) {
        // ignore parse errors
      }
    };
  </script>
</body>
</html>
  `);
});

/* PATCH /api/posts/like - toggle like */
app.patch('/api/posts/like', async (req, res) => {
  try {
    let { userId, postId, likerId } = req.body;
    const ownerId = req.body.ownerId || userId;

    if (!ownerId || !likerId || !postId) {
      return res.status(400).json({ error: 'ownerId/userId, likerId and postId are all required' });
    }

    async function scanAndUpdate(ownerIdToCheck) {
      const slots = await db.collection('user_slots').find({ userId: ownerIdToCheck }).toArray();

      for (const slot of slots) {
        let listName = null;
        let idx = -1;

        if (Array.isArray(slot.postList)) {
          idx = slot.postList.findIndex(p => p.postId === postId);
          if (idx > -1) listName = 'postList';
        }

        if (idx === -1 && Array.isArray(slot.reelsList)) {
          idx = slot.reelsList.findIndex(p => p.postId === postId);
          if (idx > -1) listName = 'reelsList';
        }

        if (idx > -1) {
          const item = slot[listName][idx];
          item.likedBy = item.likedBy || [];
          const already = item.likedBy.indexOf(likerId);
          let toggleDelta;

          if (already === -1) {
            item.likedBy.push(likerId);
            item.likeCount = (item.likeCount || 0) + 1;
            toggleDelta = +1;
          } else {
            item.likedBy.splice(already, 1);
            item.likeCount = Math.max((item.likeCount || 1) - 1, 0);
            toggleDelta = -1;
          }

          slot.updatedAt = new Date().toISOString();
          await db.collection('user_slots').replaceOne({ _id: slot._id }, slot);

          return { slot, listName, toggleDelta };
        }
      }
      return null;
    }

    // Try normal scan
    let result = await scanAndUpdate(ownerId);
    let usedFallback = false;

    // If not found, swap ownerId <-> postId and try again
    if (!result) {
      result = await scanAndUpdate(postId);
      usedFallback = !!result;
    }

    if (!result) {
      return res.status(404).json({ error: 'Post not found' });
    }

    const { slot, listName, toggleDelta } = result;

    // Broadcast update
    const allData = await getAllDataForBroadcast();
    broadcastUpdate(allData);

    return res.json({
      success: true,
      slotIndex: slot.slotIndex,
      listName,
      usedFallback,
      toggled: toggleDelta === +1,
      updatedSlot: slot
    });
  } catch (err) {
    console.error('PATCH /api/posts/like error', err);
    return res.status(500).json({ error: err.message });
  }
});

/* PATCH /api/posts/view - increment viewcount */
app.patch('/api/posts/view', async (req, res) => {
  try {
    const { userId, postId } = req.body;
    if (!userId || !postId) {
      return res.status(400).json({ error: 'userId and postId are required' });
    }

    const slots = await db.collection('user_slots').find({ userId }).toArray();

    let foundSlot = null;
    let listName = null;
    let idx = -1;

    for (const slot of slots) {
      if (Array.isArray(slot.postList)) {
        idx = slot.postList.findIndex(p => p.postId === postId);
        if (idx > -1) {
          foundSlot = slot;
          listName = 'postList';
          break;
        }
      }
      if (Array.isArray(slot.reelsList)) {
        idx = slot.reelsList.findIndex(p => p.postId === postId);
        if (idx > -1) {
          foundSlot = slot;
          listName = 'reelsList';
          break;
        }
      }
    }

    if (!foundSlot) {
      return res.status(404).json({ error: 'Post not found' });
    }

    // Increment viewcount
    const item = foundSlot[listName][idx];
    const oldVc = (typeof item.viewcount === 'number') ? item.viewcount : 0;
    item.viewcount = oldVc + 1;
    foundSlot.updatedAt = new Date().toISOString();

    await db.collection('user_slots').replaceOne({ _id: foundSlot._id }, foundSlot);

    // Broadcast update
    const allData = await getAllDataForBroadcast();
    broadcastUpdate(allData);

    return res.json({
      success: true,
      slotIndex: foundSlot.slotIndex,
      listName,
      updatedSlot: foundSlot
    });
  } catch (err) {
    console.error('PATCH /api/posts/view error', err);
    return res.status(500).json({ error: err.message });
  }
});

/* PATCH /api/posts/retention - update audienceRetention average */
app.patch('/api/posts/retention', async (req, res) => {
  try {
    const { userId, postId, retentionPercent } = req.body;
    if (!userId || !postId || retentionPercent == null) {
      return res.status(400).json({ error: 'userId, postId, and retentionPercent are required' });
    }

    const rp = parseFloat(retentionPercent);
    if (isNaN(rp) || rp < 0) {
      return res.status(400).json({ error: 'retentionPercent must be a non-negative number' });
    }

    const slots = await db.collection('user_slots').find({ userId }).toArray();

    let foundSlot = null;
    let listName = null;
    let idx = -1;

    for (const slot of slots) {
      if (Array.isArray(slot.postList)) {
        idx = slot.postList.findIndex(p => p.postId === postId);
        if (idx > -1) {
          foundSlot = slot;
          listName = 'postList';
          break;
        }
      }
      if (Array.isArray(slot.reelsList)) {
        idx = slot.reelsList.findIndex(p => p.postId === postId);
        if (idx > -1) {
          foundSlot = slot;
          listName = 'reelsList';
          break;
        }
      }
    }

    if (!foundSlot) {
      return res.status(404).json({ error: 'Post not found' });
    }

    // Compute new audienceRetention
    const item = foundSlot[listName][idx];
    const oldVc = (typeof item.viewcount === 'number') ? item.viewcount : 0;
    const oldAr = (typeof item.audienceRetention === 'number') ? item.audienceRetention : 0;
    const newAr = (oldVc + 1) > 0 ? (oldAr * oldVc + rp) / (oldVc + 1) : rp;

    item.audienceRetention = newAr;
    foundSlot.updatedAt = new Date().toISOString();

    await db.collection('user_slots').replaceOne({ _id: foundSlot._id }, foundSlot);

    // Broadcast update
    const allData = await getAllDataForBroadcast();
    broadcastUpdate(allData);

    return res.json({
      success: true,
      slotIndex: foundSlot.slotIndex,
      listName,
      updatedSlot: foundSlot
    });
  } catch (err) {
    console.error('PATCH /api/posts/retention error', err);
    return res.status(500).json({ error: err.message });
  }
});

/* PATCH /api/posts/comment - increment/decrement commentCount */
app.patch('/api/posts/comment', async (req, res) => {
  try {
    let { userId, postId, delta } = req.body;
    if (!userId || !postId || typeof delta !== 'number') {
      return res.status(400).json({ error: 'userId, postId, and numeric delta are required' });
    }
    if (![1, -1].includes(delta)) {
      return res.status(400).json({ error: 'delta must be +1 or -1' });
    }

    async function scanAndUpdate(ownerIdToCheck) {
      const slots = await db.collection('user_slots').find({ userId: ownerIdToCheck }).toArray();

      for (const slot of slots) {
        let listName = null;
        let idx = -1;

        if (Array.isArray(slot.postList)) {
          idx = slot.postList.findIndex(p => p.postId === postId);
          if (idx > -1) listName = 'postList';
        }

        if (idx === -1 && Array.isArray(slot.reelsList)) {
          idx = slot.reelsList.findIndex(p => p.postId === postId);
          if (idx > -1) listName = 'reelsList';
        }

        if (idx > -1) {
          const item = slot[listName][idx];
          const oldCc = typeof item.commentCount === 'number' ? item.commentCount : 0;
          const newCc = Math.max(oldCc + delta, 0);
          item.commentCount = newCc;
          slot.updatedAt = new Date().toISOString();

          await db.collection('user_slots').replaceOne({ _id: slot._id }, slot);
          return { slot, listName };
        }
      }
      return null;
    }

    // Try normal scan
    let result = await scanAndUpdate(userId);
    let usedFallback = false;

    // If not found, swap userId <-> postId and try again
    if (!result) {
      const realOwner = postId, realPostId = userId;
      [userId, postId] = [realOwner, realPostId];
      result = await scanAndUpdate(userId);
      usedFallback = !!result;
    }

    if (!result) {
      return res.status(404).json({ error: 'Post not found' });
    }

    const { slot, listName } = result;

    // Broadcast update
    const allData = await getAllDataForBroadcast();
    broadcastUpdate(allData);

    return res.json({
      success: true,
      slotIndex: slot.slotIndex,
      listName,
      usedFallback,
      updatedSlot: slot
    });
  } catch (err) {
    console.error('PATCH /api/posts/comment error', err);
    return res.status(500).json({ error: err.message });
  }
});

/* GET all hierarchical data */
app.get('/api/posts/all', async (req, res) => {
  try {
    const data = await getAllDataForBroadcast();
    return res.json(data);
  } catch (err) {
    console.error('GET /api/posts/all error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET posts for a user */
app.get('/api/posts/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const userSlots = await db.collection('user_slots').find({ userId }).toArray();

    let totalPosts = 0;
    let totalReels = 0;
    const slotsObj = {};

    userSlots.forEach(slot => {
      slotsObj[slot.slotIndex] = slot;
      totalPosts += slot.postCount || 0;
      totalReels += slot.reelCount || 0;
    });

    const result = {
      users_posts: {
        [userId]: {
          user_post: {}
        }
      },
      metadata: {
        userId: userId,
        totalSlots: userSlots.length,
        totalPosts: totalPosts,
        totalReels: totalReels,
        lastUpdated: new Date().toISOString()
      }
    };

    for (const [slotIndex, doc] of Object.entries(slotsObj)) {
      const docName = `${userId}_${slotIndex}`;
      result.users_posts[userId].user_post[docName] = doc;
    }

    return res.json(result);
  } catch (err) {
    console.error('GET /api/posts/user/:userId error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET following content for a user (flattened) */
app.get('/api/posts/following/:userId', async (req, res) => {
  try {
    const { userId } = req.params;

    if (!userId) {
      return res.status(400).json({ error: 'userId is required' });
    }

    const followingContent = [];
    const userSlots = await db.collection('user_slots').find({ userId }).toArray();

    userSlots.forEach(slot => {
      const slotIndex = slot.slotIndex;
      const docName = `${userId}_${slotIndex}`;

      // Add posts with document mapping
      if (Array.isArray(slot.postList) && slot.postList.length > 0) {
        slot.postList.forEach(post => {
          followingContent.push({
            ...post,
            sourceDocument: docName,
            documentSlot: slotIndex,
            contentType: 'following'
          });
        });
      }

      // Add reels with document mapping
      if (Array.isArray(slot.reelsList) && slot.reelsList.length > 0) {
        slot.reelsList.forEach(reel => {
          followingContent.push({
            ...reel,
            sourceDocument: docName,
            documentSlot: slotIndex,
            contentType: 'following'
          });
        });
      }
    });

    // Sort by timestamp (newest first)
    followingContent.sort((a, b) => {
      try {
        return new Date(b.timestamp || b.savedAt || 0) - new Date(a.timestamp || a.savedAt || 0);
      } catch (e) {
        return 0;
      }
    });

    return res.json({
      success: true,
      userId: userId,
      content: followingContent,
      totalItems: followingContent.length,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('GET /api/posts/following/:userId error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET specific slot */
app.get('/api/posts/user/:userId/slot/:slotIndex', async (req, res) => {
  try {
    const { userId, slotIndex } = req.params;

    if (!userId || !slotIndex) {
      return res.status(400).json({ error: 'userId and slotIndex are required' });
    }

    const doc = await db.collection('user_slots').findOne({
      userId,
      slotIndex: parseInt(slotIndex, 10)
    });

    if (!doc) {
      return res.status(404).json({
        success: false,
        message: `No document for ${userId}_${slotIndex}`
      });
    }

    return res.json({
      success: true,
      userId,
      slotIndex,
      documentName: `${userId}_${slotIndex}`,
      doc
    });
  } catch (err) {
    console.error('GET /api/posts/user/:userId/slot/:slotIndex error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* POST /api/posts/batch-following */
/* POST /api/posts/batch-following */
app.post('/api/posts/batch-following', async (req, res) => {
  try {
    console.log('[FOLLOWING] Received batch-following request');
    const { userIds } = req.body;

    if (!userIds || !Array.isArray(userIds) || userIds.length === 0) {
      console.log('[FOLLOWING] ERROR: Invalid or missing userIds array');
      return res.status(400).json({ error: 'userIds array is required' });
    }

    console.log('[FOLLOWING] Processing batch request for users:', userIds);

    const allFollowingContent = [];

    // Get all slots for all requested users in one query
    const allUserSlots = await db.collection('user_slots').find({
      userId: { $in: userIds }
    }).toArray();

    console.log('[FOLLOWING] Found', allUserSlots.length, 'slots for', userIds.length, 'requested users');

    allUserSlots.forEach(slot => {
      const userId = slot.userId;
      const slotIndex = slot.slotIndex;
      const docName = `${userId}_${slotIndex}`;

      console.log('[FOLLOWING] Processing slot:', docName, 'postCount:', slot.postCount || 0, 'reelCount:', slot.reelCount || 0);

      // Add posts with document mapping
      if (Array.isArray(slot.postList) && slot.postList.length > 0) {
        slot.postList.forEach(post => {
          const followingPost = {
            ...post,
            sourceDocument: docName,
            documentSlot: slotIndex,
            followedUserId: userId,
            contentType: 'following',
            isReel: false
          };
          allFollowingContent.push(followingPost);
          console.log('[FOLLOWING] Added following post:', post.postId, 'from user:', userId);
        });
      }

      // Add reels with document mapping
      if (Array.isArray(slot.reelsList) && slot.reelsList.length > 0) {
        slot.reelsList.forEach(reel => {
          const followingReel = {
            ...reel,
            sourceDocument: docName,
            documentSlot: slotIndex,
            followedUserId: userId,
            contentType: 'following',
            isReel: true
          };
          allFollowingContent.push(followingReel);
          console.log('[FOLLOWING] Added following reel:', reel.postId, 'from user:', userId);
        });
      }
    });

    // Sort by engagement then timestamp (prioritize high-engagement following content)
    allFollowingContent.sort((a, b) => {
      const engagementA = (a.likeCount || 0) * 2 + (a.commentCount || 0) * 3;
      const engagementB = (b.likeCount || 0) * 2 + (b.commentCount || 0) * 3;
      if (engagementA !== engagementB) {
        return engagementB - engagementA;
      }
      try {
        return new Date(b.timestamp || b.savedAt || 0) - new Date(a.timestamp || a.savedAt || 0);
      } catch (e) {
        return 0;
      }
    });

    console.log('[FOLLOWING] Successfully processed', allFollowingContent.length, 'following content items');
    console.log('[FOLLOWING] Content breakdown by user:', userIds.map(uid => {
      const userContent = allFollowingContent.filter(c => c.followedUserId === uid);
      return `${uid}: ${userContent.length} items`;
    }).join(', '));

    return res.json({
      success: true,
      requestedUsers: userIds,
      content: allFollowingContent,
      totalItems: allFollowingContent.length,
      userCount: userIds.length,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('[FOLLOWING] ERROR in batch-following endpoint:', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET /api/posts/document-info/:postId */
app.get('/api/posts/document-info/:postId', async (req, res) => {
  try {
    const { postId } = req.params;

    if (!postId) {
      return res.status(400).json({ error: 'postId is required' });
    }

    // Search through all user documents to find the post
    const allSlots = await db.collection('user_slots').find({}).toArray();

    let foundDocument = null;
    let foundContent = null;

    for (const slot of allSlots) {
      const userId = slot.userId;
      const slotIndex = slot.slotIndex;
      const docName = `${userId}_${slotIndex}`;

      // Check in postList
      if (Array.isArray(slot.postList) && slot.postList.length > 0) {
        const foundPost = slot.postList.find(post => post.postId === postId);
        if (foundPost) {
          foundDocument = docName;
          foundContent = { ...foundPost, isReel: false };
          break;
        }
      }

      // Check in reelsList
      if (Array.isArray(slot.reelsList) && slot.reelsList.length > 0) {
        const foundReel = slot.reelsList.find(reel => reel.postId === postId);
        if (foundReel) {
          foundDocument = docName;
          foundContent = { ...foundReel, isReel: true };
          break;
        }
      }
    }

    if (!foundDocument) {
      return res.status(404).json({
        error: 'Post not found',
        postId: postId
      });
    }

    return res.json({
      success: true,
      postId: postId,
      sourceDocument: foundDocument,
      content: foundContent,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('GET /api/posts/document-info/:postId error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* GET /api/posts/following?userIds=id1,id2 */
app.get('/api/posts/following', async (req, res) => {
  try {
    const { userIds } = req.query;

    if (!userIds) {
      return res.status(400).json({ error: 'userIds query parameter is required' });
    }

    const followedUserIds = userIds.split(',').map(s => s.trim()).filter(Boolean);

    if (followedUserIds.length === 0) {
      return res.json({ posts: [], reels: [] });
    }

    const allPosts = [];
    const allReels = [];
    const userStats = {};

    // Get all slots for all followed users in one query
    const allUserSlots = await db.collection('user_slots').find({
      userId: { $in: followedUserIds }
    }).toArray();

    // Process each slot
    allUserSlots.forEach(slot => {
      const userId = slot.userId;
      const slotIndex = slot.slotIndex;

      if (!userStats[userId]) {
        userStats[userId] = { posts: 0, reels: 0, latestPostTime: null, latestReelTime: null };
      }

      // Add posts from this slot
      if (Array.isArray(slot.postList)) {
        slot.postList.forEach(post => {
          const enhancedPost = {
            ...post,
            slotIndex: slotIndex,
            fetchedAt: new Date().toISOString(),
            userId: userId
          };

          allPosts.push(enhancedPost);
          userStats[userId].posts++;

          const postTime = new Date(post.timestamp || post.savedAt || 0);
          if (!userStats[userId].latestPostTime || postTime > userStats[userId].latestPostTime) {
            userStats[userId].latestPostTime = postTime;
          }
        });
      }

      // Add reels from this slot
      if (Array.isArray(slot.reelsList)) {
        slot.reelsList.forEach(reel => {
          const enhancedReel = {
            ...reel,
            slotIndex: slotIndex,
            fetchedAt: new Date().toISOString(),
            userId: userId
          };

          allReels.push(enhancedReel);
          userStats[userId].reels++;

          const reelTime = new Date(reel.timestamp || reel.savedAt || 0);
          if (!userStats[userId].latestReelTime || reelTime > userStats[userId].latestReelTime) {
            userStats[userId].latestReelTime = reelTime;
          }
        });
      }
    });

    // Sort by timestamp (newest first)
    allPosts.sort((a, b) => {
      const timeA = new Date(a.timestamp || a.savedAt || 0);
      const timeB = new Date(b.timestamp || b.savedAt || 0);
      return timeB - timeA;
    });

    allReels.sort((a, b) => {
      const timeA = new Date(a.timestamp || a.savedAt || 0);
      const timeB = new Date(b.timestamp || b.savedAt || 0);
      return timeB - timeA;
    });

    return res.json({
      success: true,
      posts: allPosts,
      reels: allReels,
      metadata: {
        followedUsersCount: followedUserIds.length,
        totalPosts: allPosts.length,
        totalReels: allReels.length,
        fetchedAt: new Date().toISOString(),
        userStats: userStats
      }
    });
  } catch (err) {
    console.error('GET /api/posts/following (query) error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* DELETE /api/posts/clear - delete everything */
app.delete('/api/posts/clear', async (req, res) => {
  try {
    const result = await db.collection('user_slots').deleteMany({});

    // Broadcast empty data to all clients
    const emptyData = {
      users_posts: {},
      metadata: {
        totalUsers: 0,
        totalSlots: 0,
        totalPosts: 0,
        totalReels: 0,
        lastUpdated: new Date().toISOString()
      }
    };
    broadcastUpdate(emptyData);

    return res.status(200).json({
      success: true,
      message: 'All user_slots cleared',
      deletedCount: result.deletedCount,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('DELETE /api/posts/clear error', err);
    return res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

/* Graceful shutdown */
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    console.log('Received', sig, 'shutting down...');
    try {
      if (client && client.close) {
        await client.close();
        console.log('MongoDB connection closed');
      }
    } catch (e) {
      console.warn('DB close error', e);
    }
    process.exit(0);
  });
}

/* Start server */
async function startServer() {
  try {
    await initMongo();

    app.listen(PORT, HOST, () => {
      console.log(`🚀 Server listening on http://${HOST}:${PORT}/`);
    });

    app.on('error', err => {
      console.error('Server error:', err);
      process.exit(1);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();
