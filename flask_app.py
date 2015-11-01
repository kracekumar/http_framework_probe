# -*- coding: utf-8 -*-

import datetime

import ujson as json

from flask import Flask, Response, request
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

import redis
import pika

from schematics.models import Model
from schematics.types import StringType
from schematics.types.compound import ListType
from schematics.exceptions import ValidationError

metadata = sa.MetaData()
app = Flask(__name__)
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
def is_token_in_cache(token):
    """Check token is in cache.
    """
    conn = redis.StrictRedis('localhost', 6379)
    print('Checking token: {}'.format(token))
    return conn.sismember('access_tokens', token)


def is_valid_token(token):
    """Check given token is valid by quering redis and db when
    store doesn't have.
    """
    return is_token_in_cache(token)


def get_rabbitmq_channel():
    """Get rabbitmq channel.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost'))
    channel = connection.channel()
    return channel


def enqueue(queue, data):
    """Enqueue data to given queue
    """
    channel = get_rabbitmq_channel()
    return channel.basic_publish(body=data, routing_key=queue, exchange='')


def update_external_systems(data):
    """Once a post is created enqueue to `search`, `followers` queue
    """
    json_data = json.dumps(data).encode('utf-8')
    print("enqueuing to search")
    enqueue(queue="search", data=bytes(json_data))
    print("enqueuing to followers")
    enqueue(queue="followers", data=bytes(json_data))


def get_engine():
    """Connection pool aiopg does connection pooling by default.
    For fair comparision connectio is enabled.
    """
    engine = sa.create_engine(
        'postgresql://krace:@localhost/http_framework_probe',
        pool_size=10, max_overflow=10)
    return engine


def get_user(token):
    engine = get_engine()
    conn = engine.connect()
    try:
        res = conn.execute(users_table.select().where(
            users_table.c.access_token == token))
        user = res.fetchone()
        return user
    finally:
        conn.close()


def create_post(data, user):
    """Clean post body, create new post, handle validations and return
    response.
    """
    validator = PostValidator(data)
    try:
        validator.validate()
        print("Validated data")
        engine = get_engine()
        conn = engine.connect()
        # Tables are created separately
        try:
            # Insert post to db
            res = conn.execute(posts_table.insert().returning(
                posts_table.columns.id, posts_table.columns.title,
                posts_table.columns.markdown_body,
                posts_table.columns.tags,
                posts_table.columns.post_by).values(
                    title=validator.title,
                    markdown_body=validator.markdown_body,
                    tags=validator.tags,
                    post_by=user['id'],
                created_at=datetime.datetime.now()))

            record = res.first()

            body = {b'id': record[0], b'title': record[1],
                    b'markdown_body': record[2],
                    b'tags': record[3], b'post_by': record[4]}

            # Enqueue to external systems
            update_external_systems(data=body)
            return make_reponse(status=201, body=body)
        finally:
            conn.close()
    except ValidationError as e:
        return make_reponse(status=400, body={'error': e.messages})


def make_reponse(body, status=200, content_type='application/json'):
    """Make HTTP Response.
    """
    json_data = json.dumps(body).encode('utf-8')
    return Response(status=status, response=bytes(json_data),
                    content_type=content_type)


@app.route('/', methods=['POST'])
def create_post_view():
    print('-'*80)
    raw_token = request.headers.get('Authorization')
    if raw_token:
        token = raw_token.split("Token")[-1].strip()
        present = is_valid_token(token)
        if present:
            data = request.json
            print("Received post data {}".format(data))
            user = get_user(token=token)
            resp = create_post(data=data, user=user)
            return resp
        return Response(status=401, response=b'Forbidden')
    return Response(status=400, response=b'Invalid Token')


application = app
if __name__ == "__main__":
    app.run(debug=True)
