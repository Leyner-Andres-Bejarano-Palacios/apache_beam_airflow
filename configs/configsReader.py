#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import os
import configparser




class ConfigsReader:
    def __init__(self):
        self.__vName = self.__class__.__name__
        self._vConfig = self.fn_cargar_configFile('config.ini')
    
    def fn_cargar_configFile(self,rutaPropiedades):
        config = configparser.ConfigParser()
        config.read(rutaPropiedades)
        return config


    @staticmethod
    def fn_get_configVar(varName):
        path_current_directory = os.path.dirname(__file__)
        path_config_file = os.path.join(path_current_directory, '', 'config.ini')
        config = configparser.ConfigParser()
        config.read(path_config_file)
        return config['configService'][varName]                        