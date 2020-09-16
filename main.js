'use strict';

const http = require('http');
const mysqlx = require('@mysql/xdevapi');

const port = process.env.PORT || 9999;
const statusOk = 200;
const statusNoContent = 204;
const statusBadRequest = 400;
const statusNotFound = 404;
const statusInternalServerError = 500;
const schema = 'social';

const client = mysqlx.getClient({
  user: 'app',
  password: 'pass',
  host: '0.0.0.0',
  port: 33060
});

function sendResponse(response, {status = statusOk, headers = {}, body = null}) {
  Object.entries(headers).forEach(function ([key, value]) {
    response.setHeader(key, value);
  });
  response.writeHead(status);
  response.end(body);
}

function sendJSON(response, body) {
  sendResponse(response, {
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
}

function map(columns) {
  return row => row.reduce((res, value, i) => ({...res, [columns[i].getColumnLabel()]: value}), {});
}

const methods = new Map();

methods.set('/posts.get', async ({response, db}) => {
  const table = await db.getTable('posts');
  const result = await table.select(['id', 'content','likes','created'])
    .orderBy('id DESC')
    .where('removed = FALSE')
    .bind('removed', 'FALSE')
    .execute();

  const data = result.fetchAll();
  const columns = result.getColumns();
  const posts = data.map(map(columns));
  sendJSON(response, posts);
});

methods.set('/posts.getById', async ({response, searchParams,db}) => {
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const table = db.getTable('posts');
  const result = await table.select(['id', 'content','likes','created'])
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .bind('removed', 'FALSE')
    .execute();
  const data = result.fetchAll();
  result.getAffectedItemsCount();
  const columns = result.getColumns();
  const posts = data.map(map(columns));
  if ( posts.length === 0 ) {
    sendResponse(response, {status: statusNotFound});
    return;
  }
  sendJSON(response, posts[0]);
});

methods.set('/posts.post', async ({response, searchParams,db}) => {
  if (!searchParams.has('content')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const content = searchParams.get('content');

  const table = db.getTable('posts');
  await table.insert('content').values(content)
    .execute();
  const newRes = await table.select(['id', 'content','likes','created'])
    .orderBy('id DESC')
    .where('removed = FALSE')
    .bind('removed', 'FALSE')
    .execute();
  const data = newRes.fetchAll();
  const columns = newRes.getColumns();
  const posts = data.map(map(columns));
  sendJSON(response, posts[0]);
});

methods.set('/posts.edit', async ({response, searchParams,db}) => {
  
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  if (!searchParams.has('content')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }
  let content = searchParams.get('content');
  const table = db.getTable('posts');
  
  const res = await table.select(['id', 'content','likes','created'])
    .where('removed = FALSE')
    .bind('removed', 'FALSE')
    .execute();
  const newData = res.fetchAll();
  const newColumns = res.getColumns();
  const newPosts = newData.map(map(newColumns));
  if (newPosts.filter(idy => idy.id === id).length === 0) {
    sendResponse(response, {status: statusNotFound});
    return;
  }
  await table.update()
    .set('content',content)
    .where('id = :id && removed = FALSE')
    .bind('removed', 'FALSE')
    .bind('id', id)
    .execute();
  const newRes = await table.select(['id', 'content','likes','created'])
    .where('removed = FALSE')
    .bind('removed', 'FALSE')
    .execute();
  const data = newRes.fetchAll();
  const columns = newRes.getColumns();
  const posts = data.map(map(columns)).filter(idx => idx.id = id);

  if ( posts.length === 0 ) {
    sendResponse(response, {status: statusNotFound});
    return;
  }
  content = '';
  sendJSON(response, posts[0]);
});

methods.set('/posts.delete', async ({response, searchParams, db}) => {
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const table = await db.getTable('posts');
  const result = await table.update()
    .set('removed', true)
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .execute();

  const removed = result.getAffectedItemsCount();
  if (removed === 0) {
    sendResponse(response, {status: statusNotFound});
    return;
  } else if (removed === 1) {
    const newRes = await table.select(['id','content','likes','created'])
      .where('removed = TRUE')
      .execute();
    const newData = newRes.fetchAll();
    const newColumns = newRes.getColumns();
    const newPosts = newData.map(map(newColumns)).filter(ix => ix.id === id);
    sendJSON(response, newPosts[0]);
  }
  sendResponse(response, {status: statusNoContent});
});

methods.set('/posts.restore', async ({response, searchParams,db}) => {
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }
  const table = await db.getTable('posts');
  const result = await table.select(['id','content','likes','created'])
    .where('removed = TRUE')
    .bind('removed', 'TRUE')
    .execute();
  const data = result.fetchAll();
  const columns = result.getColumns();
  const posts = data.map(map(columns));
  if (posts.filter(iy => iy.id === id).length === 0) {
    sendResponse(response,{status:statusNotFound});
    return;
  }
  await table.update()
    .set('removed', false)
    .where('id = :id && removed = TRUE')
    .bind('id', id)
    .bind('removed = TRUE')
    .execute();
  const res = await table.select(['id', 'content','likes','created'])
    .where('removed = FALSE')
    .bind('removed', 'FALSE')
    .execute();
  const newData = res.fetchAll();
  const newColumns = res.getColumns();
  const newPosts = newData.map(map(newColumns)).filter(xx => xx.id === id);
  sendJSON(response,newPosts[0]);
});
methods.set('/posts.like', async ({response, searchParams,db}) => {
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }
  const table = await db.getTable('posts');
  const res = await table.select(['id', 'content','likes','created'])
    .where('removed = FALSE')
    .execute();
  const newData = res.fetchAll();
  const newColumns = res.getColumns();
  const newPosts = newData.map(map(newColumns)).filter(aa => aa.id === id);
  if (newPosts.length === 0) {
    sendResponse(response, {status: statusNotFound});
    return;
  }
  const result = await table.select(['likes'])
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .bind('removed = FALSE')
    .execute();
  let data = result.fetchAll()[0][0];
  data = data + 1;
  await table.update()
    .set('likes',data)
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .bind('removed = FALSE')
  .execute();
  const lastRes = await table.select(['id','content','likes','created'])
    .where('removed = FALSE')
    .bind('removed = FALSE')
    .execute();
  const lastData = lastRes.fetchAll();
  const lastColumns = lastRes.getColumns();
  const lastPosts = lastData.map(map(lastColumns)).filter(bb => bb.id === id);
  sendJSON(response,lastPosts[0]); 
});
methods.set('/posts.dislike', async ({response, searchParams,db}) => {
  if (!searchParams.has('id')) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }

  const id = Number(searchParams.get('id'));
  if (Number.isNaN(id)) {
    sendResponse(response, {status: statusBadRequest});
    return;
  }
  const table = await db.getTable('posts');
  const res = await table.select(['id', 'content','likes','created'])
    .where('removed = FALSE')
    .execute();
  const newData = res.fetchAll();
  const newColumns = res.getColumns();
  const newPosts = newData.map(map(newColumns)).filter(aa => aa.id === id);
  if (newPosts.length === 0) {
    sendResponse(response, {status: statusNotFound});
    return;
  }
  const result = await table.select(['likes'])
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .bind('removed = FALSE')
    .execute();
  let data = result.fetchAll()[0][0];
  if (data >= 0) {
    data = data - 1;
  }
  await table.update()
    .set('likes',data)
    .where('id = :id && removed = FALSE')
    .bind('id', id)
    .bind('removed = FALSE')
    .execute();
  const lastRes = await table.select(['id','content','likes','created'])
    .where('removed = FALSE')
    .bind('removed = FALSE')
    .execute();
  const lastData = lastRes.fetchAll();
  const lastColumns = lastRes.getColumns();
  const lastPosts = lastData.map(map(lastColumns)).filter(bb => bb.id === id);
  sendJSON(response,lastPosts[0]); 
});
const server = http.createServer(async (request, response) => {
  const {pathname, searchParams} = new URL(request.url, `http://${request.headers.host}`);

  const method = methods.get(pathname);
  if (method === undefined) {
    sendResponse(response, {status: statusNotFound});
    return;
  }

  let session = null;
  try {
    session = await client.getSession();
    const db = await session.getSchema(schema);

    const params = {
      request,
      response,
      pathname,
      searchParams,
      db,
    };

    await method(params);
  } catch (e) {
    sendResponse(response, {status: statusInternalServerError});
  } finally {
    if (session !== null) {
      try {
        await session.close();
      } catch (e) {
        console.log(e);
      }
    }
  }
});

server.listen(port);
