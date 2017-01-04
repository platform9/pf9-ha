#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved
#
from ConfigParser import ConfigParser
from flask import Flask, request, jsonify, g
from context import error_handler
from hamgr.exceptions import AggregateNotFound, ClusterNotFound
import logging

LOG = logging.getLogger(__name__)
app = Flask(__name__)
app.debug = True
CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}


def get_provider():
    provider = getattr(g, '_provider', None)

    if provider is None:
        # TODO: Make this part of config
        provider_name = 'nova'
        pkg = __import__('hamgr.providers.%s' % provider_name)
        conf = ConfigParser()
        conf.read(['/etc/pf9/hamgr/hamgr.conf'])
        module = getattr(pkg.providers, provider_name)
        provider = module.get_provider(conf)
        g._provider = provider
    return provider


@app.route('/v1/ha', methods=['GET'])
@error_handler
def get_all():
    provider = get_provider()
    status = provider.get(None)
    return jsonify(status=status)


@app.route('/v1/ha/<int:aggregate_id>', methods=['GET'])
@error_handler
def get_status(aggregate_id):
    try:
        provider = get_provider()
        status = provider.get(aggregate_id)
        return jsonify(status=status)

    except AggregateNotFound:
        LOG.error('Aggregate %s was not found', aggregate_id)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<int:aggregate_id>/<action>', methods=['PUT'])
@error_handler
def update_status(aggregate_id, action):
    if not isinstance(action, basestring):
        return jsonify(dict(error='Not Found')), 400, CONTENT_TYPE_HEADER

    action = action.lower()
    if action not in ['enable', 'disable']:
        return jsonify(dict(error='Invalid action')), 400, CONTENT_TYPE_HEADER

    try:
        provider = get_provider()
        provider.put(aggregate_id, action)
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    except AggregateNotFound:
        LOG.error('Aggregate %s was not found', aggregate_id)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<uuid:host_id>', methods=['POST'])
@error_handler
def update_host_status(host_id):
    event = request.get_json().get('event', None)
    event_details = request.get_json().get('event_details', {})
    provider = get_provider()
    if event and event == 'host-down':
        masakari_notified = provider.host_down(event_details)
    elif event and event == 'host-up':
        masakari_notified = provider.host_up(event_details)
    else:
        LOG.warn('Invalid request')
        return jsonify(dict(success=False)), 422, CONTENT_TYPE_HEADER
    if masakari_notified:
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    else:
        return jsonify(dict(success=False)), 403, CONTENT_TYPE_HEADER


def app_factory(global_config, **local_conf):
    return app
