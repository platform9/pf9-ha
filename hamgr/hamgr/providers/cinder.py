# Copyright (c) 2025 Platform9 Systems Inc.


import json
import logging
import random
import subprocess
import threading
import time
import os
import uuid
from datetime import datetime
from datetime import timedelta
import tempfile

import requests
from keystoneauth1 import loading
from keystoneauth1 import session
from cinderclient import client as cinder_client

import hamgr
from hamgr.common import utils
from hamgr.db import api as db_api
from hamgr.providers.provider import Provider
from shared import constants
from shared.exceptions import ha_exceptions
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class CinderProvider(Provider):
    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'username')
        self._passwd = config.get('keystone_middleware', 'password')
        self._auth_url = config.get('keystone_middleware', 'auth_url')
        self._tenant = config.get('keystone_middleware', 'project_name')
        self._region = config.get('nova', 'region')
        self._token = None
        self._config = config
        self._du_name = config.get('DEFAULT', 'customer_shortname')
        
        try:
            self._cinder_db_connection = config.get('cinder', 'sqlconnectURI')
            LOG.info(f"Found cinder database connection in configuration")
        except Exception as e:
            LOG.warning(f"Cinder database connection not found in configuration: {str(e)}")
            self._cinder_db_connection = None
        
        self.cinder_events_processing_lock = threading.Lock()
        self.cinder_events_processing_running = False
        
        self._event_report_threshold_seconds = 30
        setting_seconds = config.getint('DEFAULT', 'event_report_threshold_seconds')
        if setting_seconds <= 0:
            raise ha_exceptions.ConfigException('invalid setting in configuration file : '
                                              'event_report_threshold_seconds '
                                              'should be bigger than 0')
        else:
            self._event_report_threshold_seconds = setting_seconds
        LOG.debug('event report threshold seconds is %s',
                 str(self._event_report_threshold_seconds))

    def _get_v3_token(self):
        self._token = utils.get_token(self._auth_url,
                                    self._tenant, self._username,
                                    self._passwd, self._token, self._region)
        return self._token

    def _get_cinder_client(self):
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(
            auth_url=self._auth_url,
            username=self._username,
            password=self._passwd,
            project_name=self._tenant,
            user_domain_id='default',
            project_domain_id='default'
        )
        sess = session.Session(auth=auth)
        return cinder_client.Client('3', session=sess, region_name=self._region)

    def _get_cinder_hosts(self):
        cinder_hosts = []
        try:
            client = self._get_cinder_client()
            services = client.services.list(binary='cinder-volume')
            for service in services:
                cinder_hosts.append(service.host)
            return cinder_hosts
        except Exception as e:
            LOG.exception(f"Error getting cinder hosts: {str(e)}")
            return []

    def _get_cinder_availability_zones(self):
        try:
            client = self._get_cinder_client()
            azs = client.availability_zones.list()
            az_names = [az.zoneName for az in azs]
            LOG.debug(f"Found cinder availability zones: {az_names}")
            if not az_names:
                LOG.error("No cinder availability zones found")
                return []
            return az_names
        except Exception as e:
            LOG.exception(f"Error getting cinder availability zones: {str(e)}")
            return []

    def _get_cinder_backends(self, cinder_host):
        try:
            client = self._get_cinder_client()
            services = client.services.list(host=cinder_host, binary='cinder-volume')
            backends = []
            for service in services:
                if '@' in service.binary:
                    backend_name = service.binary.split('@')[1]
                    backends.append(backend_name)
                elif '@' in service.host:
                    backend_name = service.host.split('@')[1]
                    backends.append(backend_name)
            return backends
        except Exception as e:
            LOG.exception(f"Error getting cinder backends for host {cinder_host}: {str(e)}")
            return []

    def _get_other_hosts_with_same_backend(self, cinder_host, backend_name):
        try:
            client = self._get_cinder_client()
            services = client.services.list(binary='cinder-volume')
            other_hosts = []
            
            for service in services:
                service_backend = None
                if '@' in service.binary:
                    service_backend = service.binary.split('@')[1]
                elif '@' in service.host:
                    service_backend = service.host.split('@')[1]
                
                if (service_backend == backend_name and 
                    service.host != cinder_host and 
                    service.state == 'up'):
                    other_hosts.append(service.host)
            
            return other_hosts
        except Exception as e:
            LOG.exception(f"Error finding other hosts with backend {backend_name}: {str(e)}")
            return []

    def _get_volumes_for_migration(self, cinder_host):
        try:
            client = self._get_cinder_client()
            
            search_opts = {'all_tenants': True}
            all_volumes = client.volumes.list(detailed=True, search_opts=search_opts)
            
            # Filter volumes that match our cinder_host
            host_volumes = []
            for vol in all_volumes:
                if hasattr(vol, 'os-vol-host-attr:host') and cinder_host in vol._info['os-vol-host-attr:host']:
                    host_volumes.append(vol)
                    LOG.debug(f"Found volume {vol.id} on host {vol._info['os-vol-host-attr:host']}")
            
            LOG.debug(f"Found {len(host_volumes)} volumes on host {cinder_host}")
            all_volumes = host_volumes
            
            unique_volumes = {}
            for vol in all_volumes:
                unique_volumes[vol.id] = vol
            
            all_volumes = list(unique_volumes.values())
            
            try:
                loader = loading.get_plugin_loader('password')
                auth = loader.load_from_options(
                    auth_url=self._auth_url,
                    username=self._username,
                    password=self._passwd,
                    project_name=self._tenant,
                    user_domain_id='default',
                    project_domain_id='default'
                )
                sess = session.Session(auth=auth)
                from keystoneclient.v3 import client as keystone_client
                keystone = keystone_client.Client(session=sess)
                
                services_project = None
                for project in keystone.projects.list():
                    if project.name == 'services':
                        services_project = project.id
                        break
                
                if services_project:
                    # Get all volumes in the services project
                    service_opts = {
                        'all_tenants': True,
                        'project_id': services_project
                    }
                    LOG.debug(f"Searching for glance volumes in services project: {service_opts}")
                    service_volumes = client.volumes.list(detailed=True, search_opts=service_opts)
                    
                    # Filter volumes that match our cinder_host
                    existing_ids = [v.id for v in all_volumes]
                    for vol in service_volumes:
                        if (hasattr(vol, 'os-vol-host-attr:host') and 
                            cinder_host in vol._info['os-vol-host-attr:host'] and 
                            vol.id not in existing_ids):
                            all_volumes.append(vol)
                            LOG.info(f"Found glance volume: {vol.id} on host {vol._info['os-vol-host-attr:host']} in services project")
            except Exception as e:
                LOG.exception(f"Error searching for volumes in services project: {str(e)}")
            
            return all_volumes
        except Exception as e:
            LOG.exception(f"Error getting volumes on host {cinder_host}: {str(e)}")
            return []

    def _get_backend_pools(self):
        try:
            client = self._get_cinder_client()
            pools = client.pools.list(detailed=True)
            pool_names = [pool.name for pool in pools]
            LOG.info(f"Found {len(pool_names)} backend pools: {', '.join(pool_names)}")
            return pool_names
        except Exception as e:
            LOG.exception(f"Error getting backend pools: {str(e)}")
            return []
    
    def _find_pool_for_migration(self, backend_name, backend_config, pool_names):
        LOG.info(f"Looking for pools with backend '{backend_name}' and config '{backend_config}'")
        
        # Find pools that match both the backend name and configuration
        # Format is typically: <uuid>@<backend_config>#<backend_name>
        matching_pools = [pool for pool in pool_names
                         if '#' in pool and '@' in pool
                         and pool.split('#')[1] == backend_config
                         and pool.split('@')[1].split('#')[0] == backend_name]
        
        LOG.info(f"Found {len(matching_pools)} pools with backend '{backend_name}' and config '{backend_config}'")
        
        if not matching_pools:
            LOG.error(f"No suitable pools found for migration from {backend_name}")
            return None
            
        # Select a random pool from the matching pools to distribute volumes
        target_pool = random.choice(matching_pools)
        LOG.info(f"Selected migration target pool: {target_pool} from {len(matching_pools)} available pools")
        return target_pool
    
    def _migrate_volumes(self, cinder_host, backend_name, new_host):
        try:
            LOG.info(f"Migrating volumes from {cinder_host}@{backend_name} to {new_host}@{backend_name}")
            
            volumes = self._get_volumes_for_migration(cinder_host)
            if not volumes:
                LOG.info(f"No volumes found to migrate from {cinder_host}@{backend_name}")
                return True
            
            volume_ids = [v.id for v in volumes]
            LOG.info(f"Found {len(volumes)} volumes to migrate: {', '.join(volume_ids)}")
            
            # Get the current host format from the volumes
            source_host_format = None
            if volumes and hasattr(volumes[0], 'os-vol-host-attr:host'):
                source_host_format = getattr(volumes[0], 'os-vol-host-attr:host')
                LOG.info(f"Detected original volume host format: {source_host_format}")
            else:
                LOG.warning("Could not detect host format from volumes")
                return False
            
            # Get the list of backend pools
            pool_names = self._get_backend_pools()
            
            # Extract the backend configuration from the source host format
            if '@' in source_host_format and '#' in source_host_format:
                source_config = source_host_format.split('@')[1].split('#')[1]
                LOG.info(f"Extracted backend configuration '{source_config}' from source host format")
            else:
                LOG.error(f"Could not extract backend configuration from source host format: {source_host_format}")
                return False
            
            # Find a pool to migrate to
            target_pool = self._find_pool_for_migration(backend_name, source_config, pool_names)
            
            # Define source and target formats based on pool availability
            if source_host_format and target_pool:
                # We found a pool to migrate to - use it directly
                source_host_formats = [source_host_format]
                target_host_formats = [target_pool]
                LOG.info(f"Migrating from {source_host_format} to {target_pool}")
            else:
                # If we can't find a suitable pool, we can't proceed with migration
                LOG.error(f"No suitable pool found for backend {backend_name}. Cannot proceed with migration.")
                return False
            
            LOG.info(f"Using cinder-manage to migrate volumes to {target_host_formats[0]}")
            
            os_auth_url = self._auth_url
            os_username = self._username
            os_password = self._passwd
            os_project_name = self._tenant
            os_region_name = self._region
            
            cinder_db_connection = self._get_cinder_db_connection()
            if not cinder_db_connection:
                LOG.error("Could not determine cinder database connection information")
                return False
            
            LOG.debug(f"Using cinder database connection: {cinder_db_connection}")
            
            os.makedirs('/etc/pf9', exist_ok=True)
            
            cinder_conf_path = '/etc/pf9/cinder.conf'
            with open(cinder_conf_path, 'w') as conf_file:
                conf_file.write("[database]\n")
                conf_file.write(f"connection = {cinder_db_connection}\n")
            
            LOG.info(f"Created cinder.conf at {cinder_conf_path}")
            
            env = os.environ.copy()
            env["OS_AUTH_URL"] = os_auth_url
            env["OS_USERNAME"] = os_username
            env["OS_PASSWORD"] = os_password
            env["OS_PROJECT_NAME"] = os_project_name
            env["OS_TENANT_NAME"] = os_project_name  # For backward compatibility
            env["OS_REGION_NAME"] = os_region_name
            env["OS_IDENTITY_API_VERSION"] = "3"
            env["OS_AUTH_TYPE"] = "password"
            env["OS_USER_DOMAIN_NAME"] = "Default"
            env["OS_PROJECT_DOMAIN_NAME"] = "Default"
            env["CINDER_CONF"] = cinder_conf_path  # Point to our config file
            
            # Try each source host format with each target host format until one works
            success = False
            for source_host in source_host_formats:
                for target_host in target_host_formats:
                    cmd = f"cinder-manage --config-file {cinder_conf_path} volume update_host --currenthost {source_host} --newhost {target_host}"
                    LOG.info(f"Executing command: {cmd}")
                    
                    result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        LOG.info(f"Database update for migration from {source_host} to {target_host} completed")
                        LOG.debug(f"cinder-manage output: {result.stdout}")
                        
                        # Give time for changes to propagate
                        time.sleep(2)
                        
                        verification_success = self._verify_volume_migration(volumes, target_host)
                        
                        if verification_success:
                            LOG.info(f"Successfully verified migration from {source_host} to {target_host}")
                            success = True
                            break
                        else:
                            LOG.warning(f"Database update succeeded but volumes still show old host with format {source_host}->{target_host}. Trying other host format combinations.")
                    else:
                        LOG.warning(f"Failed to migrate volumes with host format {source_host} to {target_host}: {result.stderr}")
                
                if success:
                    break
            
            if not success:
                LOG.error(f"Failed to migrate volumes from {cinder_host}@{backend_name} to {new_host}@{backend_name} after trying all host format combinations")
                
            return success
                
        except Exception as e:
            LOG.exception(f"Error during volume migration with cinder-manage: {str(e)}")
            return False
            
    def _get_cinder_db_connection(self):
        try:
            if hasattr(self, '_cinder_db_connection') and self._cinder_db_connection:
                LOG.debug(f"Using cinder database connection from configuration")
                return self._cinder_db_connection
            
            LOG.error("Cinder database connection not found in configuration")
            return None
            
        except Exception as e:
            LOG.exception(f"Error getting cinder database connection: {str(e)}")
            return None

    def _verify_volume_migration(self, volumes, target_host):
        try:
            client = self._get_cinder_client()
            
            all_migrated = True
            for volume in volumes:
                current_vol = client.volumes.get(volume.id)
                
                # The attribute name is 'os-vol-host-attr:host'
                host_attr = getattr(current_vol, 'os-vol-host-attr:host', None)
                
                base_target = target_host.split('#')[0] if '#' in target_host else target_host
                
                LOG.info(f"Checking volume {volume.id} migration: current host = {host_attr}, target = {target_host}")
                
                if host_attr and (host_attr == target_host or host_attr.startswith(base_target)):
                    LOG.info(f"Volume {volume.id} successfully migrated to {host_attr}")
                else:
                    LOG.warning(f"Volume {volume.id} not migrated to target host. Current host: {host_attr}")
                    all_migrated = False
            
            return all_migrated
            
        except Exception as e:
            LOG.exception(f"Error verifying volume migration: {str(e)}")
            return False
    
    def _is_cinder_service_active(self, host_id):
        try:
            client = self._get_cinder_client()
            services = client.services.list(host=host_id, binary='cinder-volume')
            for service in services:
                if service.host == host_id:
                    return service.state == 'up'
            return False
        except Exception as e:
            LOG.exception(f"Error checking if cinder service is active: {str(e)}")
            return False

    def process_cinder_host_events(self):
        if self.cinder_events_processing_running:
            LOG.debug('cinder events processing is already running')
            return

        with self.cinder_events_processing_lock:
            if self.cinder_events_processing_running:
                LOG.debug('cinder events processing is already running')
                return
            self.cinder_events_processing_running = True

        try:
            LOG.debug('Starting cinder host events processing')
            
            unhandled_events = db_api.get_all_unhandled_cinder_processing_events()
            if not unhandled_events:
                LOG.debug('No unhandled cinder host events found in database')
                
                # Check for standalone cinder hosts
                self._token = self._get_v3_token()
                LOG.debug('Checking for standalone cinder hosts that might be down')
                cinder_hosts = self._get_cinder_hosts()
                LOG.debug(f"Found {len(cinder_hosts)} cinder hosts: {', '.join(cinder_hosts)}")
                
                for host in cinder_hosts:
                    is_active = self._is_cinder_service_active(host)
                    if not is_active:
                        LOG.info(f"Detected standalone cinder host down: {host}")
                        event_uuid = str(uuid.uuid4())
                        LOG.info(f"Creating new cinder host down event with UUID {event_uuid} for host {host}")
                        db_api.create_cinder_processing_event(
                            event_uuid,
                            constants.EVENT_HOST_DOWN,
                            host
                        )
                
                unhandled_events = db_api.get_all_unhandled_cinder_processing_events()
                if not unhandled_events:
                    LOG.debug('Still no cinder host events found after checking standalone hosts')
                    return
            
            events = sorted(unhandled_events, key=lambda x: x.event_time)
            if not events:
                LOG.debug('No cinder host events to process')
                return
            
            event_descriptions = []
            for event in events:
                event_desc = (f"Event[uuid={event.event_uuid}, type={event.event_type}, "
                             f"host={event.host_name}, time={event.event_time}, "
                             f"status={event.notification_status}]")
                event_descriptions.append(event_desc)
            
            LOG.info('Found %d cinder host events to process: %s', 
                    len(events), ', '.join(event_descriptions))
            
            self._token = self._get_v3_token()
            time_out = timedelta(minutes=30)
            
            for event in events:
                LOG.debug('Processing cinder event %s', 
                         f"[uuid={event.event_uuid}, type={event.event_type}, host={event.host_name}]")
                event_uuid = event.event_uuid
                event_type = event.event_type
                event_time = event.event_time
                host_name = event.host_name
                
                if datetime.utcnow() - event_time > time_out:
                    LOG.warning('Aborting stale cinder event %s from %s (older than %d minutes)',
                              event_uuid, event_time, time_out.total_seconds() / 60)
                    db_api.update_cinder_processing_event(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'Event happened long time ago: %s' % str(event_time)
                    )
                    continue
                
                is_active = self._is_cinder_service_active(host_name)
                LOG.debug('Cinder host %s active status: %s', host_name, is_active)
                
                if event_type == constants.EVENT_HOST_DOWN and not is_active:
                    LOG.info('Processing cinder host down event for %s', host_name)
                    
                    backends = self._get_cinder_backends(host_name)
                    if not backends:
                        LOG.warning('No backends found for cinder host %s, aborting event processing', host_name)
                        db_api.update_cinder_processing_event(
                            event_uuid, None, None,
                            constants.STATE_ABORTED,
                            'No backends found for cinder host'
                        )
                        continue
                    
                    LOG.info(f"Found {len(backends)} backends for host {host_name}: {', '.join(backends)}")
                    
                    cinder_azs = self._get_cinder_availability_zones()
                    LOG.info(f"Cinder host {host_name} is part of availability zones: {', '.join(cinder_azs)}")
                    
                    success = True
                    for backend_name in backends:
                        LOG.info(f"Processing backend {backend_name} on host {host_name}")
                        
                        other_hosts = self._get_other_hosts_with_same_backend(host_name, backend_name)
                        if not other_hosts:
                            LOG.warning(f"No other hosts found with backend {backend_name} in up state, skipping volume migration for this backend")
                            continue
                        
                        LOG.info(f"Found {len(other_hosts)} other hosts with backend {backend_name}: {', '.join(other_hosts)}")
                        
                        # Get volumes across all projects for cross-AZ attachments and glance backend
                        volumes = self._get_volumes_for_migration(host_name)
                        if not volumes:
                            LOG.info(f"No volumes found on {host_name}@{backend_name}, skipping migration for this backend")
                            continue
                        
                        volume_ids = [v.id for v in volumes]
                        LOG.info(f"Found {len(volumes)} volumes on {host_name}@{backend_name} to migrate: {', '.join(volume_ids)}")
                        
                        new_host = other_hosts[0]
                        LOG.info(f"Selected host {new_host} as migration target for volumes from {host_name}@{backend_name}")
                        
                        migration_success = self._migrate_volumes(host_name, backend_name, new_host)
                        if not migration_success:
                            success = False
                            LOG.error(f"Failed to migrate volumes from {host_name}@{backend_name} to {new_host}@{backend_name}")
                    
                    if success:
                        LOG.info(f"Successfully processed all backends for host {host_name}, marking event {event_uuid} as finished")
                        db_api.update_cinder_processing_event(
                            event_uuid, None, None,
                            constants.STATE_FINISHED,
                            'Successfully migrated all volumes'
                        )
                    else:
                        LOG.error(f"Failed to process some backends for host {host_name}, marking event {event_uuid} as failed")
                        db_api.update_cinder_processing_event(
                            event_uuid, None, None,
                            constants.STATE_FAILED,
                            'Failed to migrate some volumes'
                        )
                
                elif event_type == constants.EVENT_HOST_UP:
                    if is_active:
                        LOG.info('Cinder host %s is back up, marking event %s as finished', host_name, event_uuid)
                        db_api.update_cinder_processing_event(
                            event_uuid, None, None,
                            constants.STATE_FINISHED,
                            'Host is back up'
                        )
                    else:
                        LOG.warning('Cinder host %s is still down, keeping event %s active', host_name, event_uuid)
                
                else:
                    LOG.warning('Unknown cinder event type %s for host %s, aborting event %s', 
                              event_type, host_name, event_uuid)
                    db_api.update_cinder_processing_event(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'Unknown event type: %s' % event_type
                    )
            
            LOG.debug('Finished processing cinder host events')
        except Exception as ex:
            LOG.exception('Unhandled exception in process_cinder_host_events: %s', str(ex))
        finally:
            with self.cinder_events_processing_lock:
                self.cinder_events_processing_running = False


def get_provider(config):
    return CinderProvider(config)
