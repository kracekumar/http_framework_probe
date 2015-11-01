# -*- coding: utf-8 -*-

import logging
import datetime
import asyncio

import ujson as json
from aiohttp import web
from aiopg.sa import create_engine

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

import aioredis
import aioamqp

from schematics.models import Model
from schematics.types import StringType
from schematics.types.compound import ListType
from schematics.exceptions import ValidationError

#logging.basicConfig(level=logging.DEBUG)
metadata = sa.MetaData()

# sa #

users_table = sa.Table('users', metadata,
                       sa.Column('id', sa.Integer, primary_key=True),
                       sa.Column('email', sa.String(255), unique=True),
                       sa.Column('access_token', sa.String(30), unique=True))

posts_table = sa.Table('posts', metadata,
                       sa.Column('id', sa.Integer, primary_key=True),
                       sa.Column('title', sa.String(255), unique=True),
                       sa.Column('markdown_body', sa.String()),
                       sa.Column('tags', ARRAY(sa.String(), dimensions=1)),
                       sa.Column('post_by', sa.ForeignKey("users.id")),
                       sa.Column('created_at', sa.DateTime()))
# Validations #


class PostValidator(Model):
    title = StringType(max_length=255, min_length=5, required=True)
    markdown_body = StringType(required=True)
    tags = ListType(StringType)


# Redis #
@asyncio.coroutine
def is_token_in_cache(token):
    """Check token is in cache.
    """
    conn = yield from aioredis.create_redis(('localhost', 6379),
                                            encoding='utf-8')
    try:
        print('Checking token: {}'.format(token))
        res = yield from conn.sismember('access_tokens', token)
        return res
    finally:
        conn.close()


def is_valid_token(token):
    """Check given token is valid by quering redis and db when
    store doesn't have.
    """
    return is_token_in_cache(token)


@asyncio.coroutine
def get_rabbitmq_channel():
    """Get rabbitmq channel.
    """
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
        channel = yield from protocol.channel()
        return channel
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return


@asyncio.coroutine
def enqueue(queue, data):
    """Enqueue data to given queue
    """
    channel = yield from get_rabbitmq_channel()
    yield from channel.publish(data, '', queue)


@asyncio.coroutine
def update_external_systems(data):
    """Once a post is created enqueue to `search`, `followers` queue
    """
    json_data = json.dumps(data).encode('utf-8')
    yield from enqueue(queue="search", data=bytes(
        json_data))
    yield from enqueue(queue="followers", data=bytes(
        json_data))


@asyncio.coroutine
def get_engine():
    #print("Get engine")
    engine = yield from create_engine(user='krace',
                                      database='http_framework_probe',
                                      host='127.0.0.1',
                                      password='')
    #print("returning engine")
    return engine


@asyncio.coroutine
def get_user(token):
    engine = yield from get_engine()
    print("getting connection object")
    with (yield from engine) as conn:
        res = yield from conn.execute(users_table.select().where(
            users_table.c.access_token == token))
        user = yield from res.fetchone()
    return user


@asyncio.coroutine
def create_post(data, user):
    """Clean post body, create new post, handle validations and return
    response.
    """
    validator = PostValidator(data)
    try:
        validator.validate()
        print("Validated data")
        engine = yield from get_engine()
        # Tables are created separately
        with (yield from engine) as conn:
            # Insert post to db
            res = yield from conn.execute(posts_table.insert().returning(
                posts_table.columns.id, posts_table.columns.title,
                posts_table.columns.markdown_body,
                posts_table.columns.tags,
                posts_table.columns.post_by).values(
                    title=validator.title,
                    markdown_body=validator.markdown_body,
                    tags=validator.tags,
                    post_by=user['id'],
                created_at=datetime.datetime.now()))

            record = yield from res.first()

            body = {b'id': record[0], b'title': record[1],
                    b'markdown_body': record[2],
                    b'tags': record[3], b'post_by': record[4]}

            # Enqueue to external systems
            yield from update_external_systems(data=body)
            return make_reponse(status=201, body=body)
    except ValidationError as e:
        yield from make_reponse(status=400, body={'error': e.messages})


def make_reponse(body, status=200, content_type='application/json'):
    """Make HTTP Response.
    """
    json_data = json.dumps(body).encode('utf-8')
    return web.Response(status=status, body=bytes(json_data),
                        content_type=content_type)


@asyncio.coroutine
def create_post_view(request):
    print('-'*80)
    raw_token = request.headers.get('Authorization')
    if raw_token:
        token = raw_token.split("Token")[-1].strip()
        present = yield from is_valid_token(token)
        if present:
            data = yield from request.json()
            print("Received post data {}".format(data))
            user = yield from get_user(token=token)
            resp = yield from create_post(data=data, user=user)
            return resp
        return web.Response(status=403, body=b'Forbidden')
    return web.Response(status=400, body=b'Invalid Token')


def run_server(app):
    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler, '0.0.0.0', 8080)
    srv = loop.run_until_complete(f)
    print('serving on', srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    finally:
        loop.close()
    # try:
    #     loop.run_forever()
    # except KeyboardInterrupt:
    #     loop.close()
    # finally:
    #     loop.run_until_complete(handler.finish_connections(1.0))
    #     srv.close()
    #     loop.run_until_complete(srv.wait_closed())
    #     loop.run_until_complete(app.finish())
    #     print("Pending tasks at exit: %s" % asyncio.Task.all_tasks(loop))
    #     loop.close()


app = web.Application()
app.router.add_route('POST', '/', create_post_view)


if __name__ == "__main__":
    run_server(app)
