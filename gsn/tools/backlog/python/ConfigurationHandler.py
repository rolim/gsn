# -*- coding: UTF-8 -*-
__author__      = "Tonio Gsell <tgsell@tik.ee.ethz.ch>"
__copyright__   = "Copyright 2010, ETH Zurich, Switzerland, Tonio Gsell"
__license__     = "GPL"
__version__     = "$Revision$"
__date__        = "$Date$"
__id__          = "$Id$"
__source__      = "$URL$"


import logging
import ConfigParser
import os
import time
import signal
import tempfile
import hashlib
from pyinotify import WatchManager, ThreadedNotifier, EventsCodes, ProcessEvent

from ScheduleHandler import ScheduleHandlerClass
from BackLogMessage import CONFIG_MESSAGE_TYPE


############################################
# Some Constants

MESSAGE_PRIORITY = 5

CONFIG_HASH_FILE = '/media/card/backlog/.backlog_config_hash'
############################################


class ConfigurationHandlerClass():
    '''
    Handles all configuration file specific changes.
    '''

    '''
    data/instance attributes:
    _logger
    _config_file
    _main_config
    _notifier
    _restart
    '''
    
    def __init__(self, parent, config_file, backlog_db=None, backlog_db_resend_hr=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self._backlogMain = parent
        self._backlog_db_default = backlog_db
        self._backlog_db_resend_hr_default = backlog_db_resend_hr
        self._restart = False
                
        self._config_file = config_file
        wm = WatchManager()
        self._notifier = ThreadedNotifier(wm, ConfigChangedProcessing(self))
        wm.add_watch(config_file, EventsCodes.FLAG_COLLECTIONS['OP_FLAGS']['IN_CLOSE_WRITE'])
        
        self._main_config = self._checkConfig(config_file)
        
        
    def start(self):
        config_file = open(self._config_file, 'r')
        config_string = config_file.read()
        config_file.close()
        
        new_config_gentime = self._compareConfigs(config_string, True)

        if new_config_gentime is not None:
            self._logger.info('configuration file has changed since last time BackLog has started')
            self._backlogMain.gsnpeer.processMsg(self.getMsgType(), new_config_gentime, ['configuration changed', config_string], MESSAGE_PRIORITY, True)
            
        self._notifier.start()
    
    
    def restart(self):
        return self._restart
        
        
    def getMsgType(self):
        return CONFIG_MESSAGE_TYPE
    
            
    def msgReceived(self, data):
        self._logger.info('new configuration received from GSN')
        
        if self._compareConfigs(data[0]) is None:
            self._logger.info('received configuration is identical to the used one -> nothing to be changed')
            self._backlogMain.gsnpeer.processMsg(self.getMsgType(), int(time.time()*1000), ['received configuration is identical to the used one -> configuration will not be changed', data[0]], MESSAGE_PRIORITY, True)
        else:
            try:
                tmp_file = tempfile.NamedTemporaryFile()
                tmp_file.write(data[0])
                self._checkConfig(tmp_file.name)
        
                # Write configuration file to disk
                config_file = open(self._config_file, 'w')
                config_file.write(data[0])
                config_file.close()
                tmp_file.close()
            except Exception, e:
                self._logger.error(e.__str__())
                self._backlogMain.incrementErrorCounter()
                self._backlogMain.gsnpeer.processMsg(self.getMsgType(), int(time.time()*1000), ['%s -> configuration will not be changed' % (e.__str__()), data[0]], MESSAGE_PRIORITY, True)
        
        
    def stop(self):
        self._notifier.stop()
        
        
    def ackReceived(self, timestamp):
        pass
        
        
    def getParsedConfig(self):
        return self._main_config
    
    
    def _compareConfigs(self, config_string, write_hash=False):
        oldhash = None
        md5 = hashlib.md5()
        md5.update(config_string)
        newhash = md5.digest()
        if os.path.isfile(CONFIG_HASH_FILE):
            hash_file = open(CONFIG_HASH_FILE, 'rb')
            oldhash = hash_file.read()
            hash_file.close()
            
        if write_hash:
            hash_file = open(CONFIG_HASH_FILE, 'wb')
            hash_file.write(newhash)
            hash_file.close()
        
        if newhash != oldhash:
            return int(os.stat(CONFIG_HASH_FILE).st_mtime*1000)
        else:
            return None
        
    
    def _checkConfig(self, config_file):
        '''
        Check configuration file for correctness.
        '''
        # read config file for other options
        config = ConfigParser.SafeConfigParser()
        config.optionxform = str # case sensitive
        config.read(config_file)
        ret = dict(config=config)

        gsn_port = None
        backlog_db = self._backlog_db_default
        backlog_db_resend_hr = self._backlog_db_resend_hr_default
        device_id = None
        tos_address = None
        tos_version = None
        dutycyclemode = None
        folder_to_check_size = None
        folder_min_free_mb = None
        oldboard = None

        try:
            # readout options from config
            for name, value in config.items('options'):
                if value.strip() != '':
                    if name == 'gsn_port':
                        gsn_port = int(value)
                    elif name == 'device_id':
                        device_id = int(value)
                    elif name == 'backlog_db':
                        backlog_db = value
                    elif name == 'backlog_db_resend_hr':
                        backlog_db_resend_hr = int(value)
                    elif name == 'tos_source_addr':
                        tos_address = value
                    elif name == 'tos_version':
                        tos_version = int(value)
                    elif name == 'duty_cycle_mode':
                        dutycyclemode = int(value)
                    elif name == 'folder_to_check_size':
                        folder_to_check_size = value
                    elif name == 'folder_min_free_mb':
                        folder_min_free_mb = int(value)
                    elif name == 'old_board':
                        oldboard = int(value)
        except ConfigParser.NoSectionError:
            raise TypeError('no [options] section specified in %s' % (config_file,))
        
        if gsn_port is None:
            gsn_port = DEFAULT_OPTION_GSN_PORT
        elif gsn_port <= 0 or gsn_port >= 65535:
            raise TypeError('gsn_port has to be in the range of 1 and 65534 (both inclusive)')
        ret.update(gsn_port=gsn_port)
                
        if device_id == None:
            raise TypeError('device_id has to be specified in the configuration file')
        if device_id >= 65535 or device_id < 0:
            raise TypeError('device_id has to be in the range of 0 and 65534 (both inclusive)')
        ret.update(device_id=device_id)
        
        if backlog_db_resend_hr <= 0:
            raise TypeError('backlog_db_resend_hr has to be a positive integer')
        ret.update(backlog_db=backlog_db)
        ret.update(backlog_db_resend_hr=backlog_db_resend_hr)
        
        if tos_address is not None:
            try:
                spl = tos_address.split('@')[1].split(':')
                device = spl[0]
                baudrate = spl[1]
            except Exception:
                raise TypeError('tos_address has to be in the form serial@DEVICE:BAUDRATE')
            if not os.path.isfile(device):
                raise TypeError('device >%s< specified in tos_address >%s< does not exist' % (device, tos_address))
            if not baudrate.isdigit():
                raise TypeError('baudrate >%s< specified in tos_address >%s< has to be an integer' % (baudrate, tos_address))
        
        if tos_address is not None and tos_version is not None and tos_version != 1 and tos_version != 2:
            raise TypeError('tos_version has to be set to 1 or 2 in config file')
        ret.update(tos_address=tos_address)
        ret.update(tos_version=tos_version)
        
        if dutycyclemode is None:
            raise TypeError('duty_cycle_mode has to be specified in the configuration file')
        elif dutycyclemode != 1 and dutycyclemode != 0:
            raise TypeError('duty_cycle_mode has to be set to 1 or 0 in config file')
        elif dutycyclemode == 1:
            duty_cycle_mode = True
        else:
            duty_cycle_mode = False
        ret.update(duty_cycle_mode=duty_cycle_mode)
                
        if not folder_to_check_size:
            raise TypeError('folder_to_check_size has to be specified in the configuration file')
        elif not os.path.isdir(folder_to_check_size):
                raise TypeError('folder_to_check_size has to be an existing directory')
        ret.update(folder_to_check_size=folder_to_check_size)
                
        if not folder_min_free_mb:
            raise TypeError('folder_min_free_mb has to be specified in the configuration file')
        elif folder_min_free_mb <= 0:
                raise TypeError('folder_min_free_mb has to be a positive number')
        ret.update(folder_min_free_mb=folder_min_free_mb)
        
        if oldboard is not None:
            if oldboard == 0:
                ret.update(oldboard=False)
            elif oldboard == 1:
                ret.update(oldboard=True)
            else:
                raise TypeError('old_board has to be set to 1 or 0 in config file')
        else:
            ret.update(oldboard=False)
        
        # get schedule section from config files
        try:
            config_schedule = config.items('schedule')
        except ConfigParser.NoSectionError:
            raise TypeError('no [schedule] section specified in %s' % (config_file,))
        
        ScheduleHandlerClass.checkConfig(duty_cycle_mode, config_schedule)
        ret.update(config_schedule=config_schedule)
        
        return ret
                
                
    def _restartBackLog(self):
        self._restart = True
        parentpid = os.getpid()
        os.kill(parentpid, signal.SIGINT)
    


class ConfigChangedProcessing(ProcessEvent):
    
    '''
    data/instance attributes:
    _logger
    _configHandler
    '''

    def __init__(self, parent):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._configHandler = parent

    def process_default(self, event):
        try:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug('%s changed' % (event.pathname,))
            
            self._configHandler._restartBackLog()
        except Exception, e:
            self._configHandler._backlogMain.incrementExceptionCounter()
            self._logger.exception(str(e))