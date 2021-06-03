#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# import sys
# sys.path.append("..")

import os
import sys
from pathlib import Path
path_root_directory = str(os.path.dirname(os.path.abspath(__file__)))[:-5]
sys.path.insert(1, path_root_directory)
import unittest
import configparser
import apache_beam as beam
from google.cloud import bigquery
from validaciones.validador import Validador
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import assert_that
from configs.configsReader import ConfigsReader
from apache_beam.testing.test_pipeline import TestPipeline

class ValidatorTest(unittest.TestCase):
    def test_completittud_flawless_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_completitud,  'afi_hash64')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))


    def test_completittud_none_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map(validador.fn_check_completitud,  'IdentificacionPension')
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':'valor IdentificacionPension no encontrado,'}]))

    def test_completittud_none_string(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':'None',\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_completitud,  'IdentificacionPension')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':'None',\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':'valor IdentificacionPension no encontrado,'}]))                                    


    def test_dateformat_flawless_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_date_format, 'FechaNacimiento', "%Y%m%d" )
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))



    def test_dateformat_incompatible_format(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'1959-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_date_format, 'FechaNacimiento', "%Y%m%d" )
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'1959-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':'FechaNacimiento tiene un formato de fecha invalida,'}]))


    def test_valueBiggerThan_flawless_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_bigger_than,    'FechaFinTemporalidad', 20131231)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))



    def test_valueBiggerThan_incorrect_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20001231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map(validador.fn_check_bigger_than,    'FechaFinTemporalidad', 20131231)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20001231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':'valor en FechaFinTemporalidad es menor que 20131231,'}]))


    def test_onlynumbers_flawless_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_numbers,  'FechaNacimiento')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))


    def test_onlynumbers_notNumeric_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'1959-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_numbers,  'FechaNacimiento')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'1959-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':'valor en FechaNacimiento no es numerico,'}])) 



    def test_onlytext_flawless_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_numbers,  'FechaNacimiento')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))


    def test_onlytext_wrong_value(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_numbers,  'FechaNacimiento')
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))


    def test_son_younger_than_parent(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'hijo',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_son_younger_than_parent)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'hijo',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))

    def test_son_younger_than_parent_wrong_valuue(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'hijo',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19791226',\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_son_younger_than_parent)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'hijo',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19791226',\
                                    'TipoPension':'2',\
                                    'validacionDetected':'edad del beneficiario hijo es mayor o igual a la del afiliado,'}]))


    def test_fn_check_pension_survivor_older_25(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_pension_survivor_older_25)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':''}]))



    def test_fn_check_pension_survivor_older_25_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'',\
                           'FechaNacimiento':'11451226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_pension_survivor_older_25)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'',\
                                    'FechaNacimiento':'11451226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'pension de sobrevivenvia a mayor de 25 años no invalido,'}]))

    def test_fn_check_pension_survivor_older_25_wrong_format(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'2020-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_pension_survivor_older_25)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'2020-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'ESTUDIANTES ENTRE 18 Y 25 AÑOS',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'FechaNacimiento tiene un formato de fecha invalida,'}]))


    def test_fn_check_age_son_ingesting(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'hijo',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_age_son_ingesting)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'hijo',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':''}]))


    def test_fn_check_age_son_ingesting_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'hijo',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_age_son_ingesting)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'hijo',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'persona beneficiaria con parentesco hijo y calidad de beneficiario sano con mas de 25 años al momento del informe,'}]))


    def test_fn_check_age_parent_ingesting(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'padre',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_age_parent_ingesting)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'padre',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':''}]))


    def test_fn_check_age_parent_ingesting_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'padre',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_age_parent_ingesting)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'padre',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'persona beneficiaria con parentesco madre o padre con mas de 95 años al momento del informe,'}]))


    def test_fn_check_age_parent_ingesting_wrong_format(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'1020-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'padre',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_age_parent_ingesting)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'1020-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'padre',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'FechaNacimiento tiene un formato de fecha invalida,'}]))


    def test_fn_check_word_lenght(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'12345678910',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'padre',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_word_lenght,"FechaNacimiento",9)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'12345678910',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'padre',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':''}]))

    def test_fn_check_word_lenght_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario2=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'S',\
                           'FechaNacimiento':'123456',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'padre',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':'19191226',\
                           'TipoPension':'3',\
                           'validacionDetected':''}]

        with TestPipeline() as p2:
            inputt2 = p2 | beam.Create(dictBeneficiario2)
            output2 = inputt2 |  beam.Map( validador.fn_check_word_lenght,"FechaNacimiento",9)
            assert_that(output2,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'S',\
                                    'FechaNacimiento':'123456',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'padre',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':'19191226',\
                                    'TipoPension':'3',\
                                    'validacionDetected':'FechaNacimiento tiene un longitud menor de 9,'}]))



    def test_fn_check_value_in_bene(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_value_in_bene,  'TipoPension',["1","2","3","4"])
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':''}]))



    def test_fn_check_value_in_bene_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'5',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_value_in_bene,  'TipoPension',["1","2","3","4"])
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'5',\
                                    'validacionDetected':'valor TipoPension no encontrado en lista,'}]))



    def test_fn_check_value_in(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_value_in,  'TipoPension',["1","2","3","4"])
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':''}]))



    def test_fn_check_value_in_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':'5',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_value_in,  'TipoPension',["1","2","3","4"])
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':'5',\
                                    'validacionDetected':'valor TipoPension no encontrado en lista,'}]))




    def test_fn_gcpm_case_different_program_retire(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_gcpm_case_different_program_retire)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':''}]))



    def test_fn_gcpm_case_different_program_retire_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'8',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_gcpm_case_different_program_retire)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'8',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':'caso GCP con pension diferente a retiro programado,'}]))


    def test_fn_gpm_less_than_1150(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_gpm_less_than_1150)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':''}]))



    def test_fn_gpm_less_than_1150_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'50',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_gpm_less_than_1150)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'50',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':'Pensiones de Garantia de Pension Minima con menos de 1150 semanas,'}]))


    def test_fn_age_to_fechaSiniestro(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_age_to_fechaSiniestro)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':''}]))


    def test_fn_age_to_fechaSiniestro_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'19791212',\
                           'Sexo':'MASCULINO',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_age_to_fechaSiniestro)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'19791212',\
                                    'Sexo':'MASCULINO',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':'edad a la fecha de siniestro es inferior a la debida para hombres en casos GPM,'}]))


    def test_fn_age_to_fechaSiniestro_wrongformat(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'1959-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'19791212',\
                           'Sexo':'MASCULINO',\
                           'fechaNacimientoAfiliado':None,\
                           'TipoPension':' 2 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_age_to_fechaSiniestro)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'1959-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'19791212',\
                                    'Sexo':'MASCULINO',\
                                    'fechaNacimientoAfiliado':None,\
                                    'TipoPension':' 2 ',\
                                    'validacionDetected':'fechaSiniestro o fechaNacimiento no poseen formato AAAMMDD,'}]))




    def test_fn_GPM_vejez_SMMLV(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_GPM_vejez_SMMLV,"1000")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'validacionDetected':''}]))


    def test_fn_GPM_vejez_SMMLV_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'50',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_GPM_vejez_SMMLV,"1000")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'50',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'validacionDetected':'cliente con pension de vejez y caso GPM con pension diferente a un SMMLV,'}]))



    def test_fn_caseGPM_saldoAgotado(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_caseGPM_saldoAgotado)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'validacionDetected':''}]))


    def test_fn_caseGPM_saldoAgotado_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19591226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 0 ',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_caseGPM_saldoAgotado)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19591226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 0 ',\
                                    'validacionDetected':'caso GPM con saldo en cero,'}]))




    def test_fn_check_pension_oldeness_women_men(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness_women_men)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))




    def test_fn_check_pension_oldeness_women_men_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'qq',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness_women_men)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'qq',\
                                    'validacionDetected':'hombre  con pension de vejez menor a 62 años de edad sin vejez anticipada,'}]))



    def test_fn_check_pension_oldeness_women_men_wrongformat(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'1020-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness_women_men)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'1020-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'FechaNacimiento tiene un formato de fecha invalida,'}]))




    def test_fn_check_pension_oldeness(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))



    def test_fn_check_pension_oldeness_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'pensionado conm edad superior a 95 años,'}]))




    def test_fn_check_pension_oldeness_wrongformat(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'1020-12-26',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'1',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_oldeness)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'1020-12-26',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'1',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'FechaNacimiento tiene un formato de fecha invalida,'}]))




    def test_fn_rentavitalicia_pencon_24(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_rentavitalicia_pencon_24)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}])) 




    def test_fn_rentavitalicia_pencon_24_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'854',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_rentavitalicia_pencon_24)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'854',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'rentavitalicia con PENCON de la PENARC diferente a 24,'}]))





    def test_fn_total_weeks_afiliafos_devoluciones(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_total_weeks_afiliafos_devoluciones)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))




    def test_fn_total_weeks_afiliafos_devoluciones_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'0',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_total_weeks_afiliafos_devoluciones)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'0',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'afiliado con Devoluciones de Saldo con semanas en cero,'}]))




    def test_fn_total_weeks_afiliafos_devoluciones_vejez(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_total_weeks_afiliafos_devoluciones_vejez)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))




    def test_fn_total_weeks_afiliafos_devoluciones_vejez_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'2000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':'1',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_total_weeks_afiliafos_devoluciones_vejez)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'2000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':'1',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'riesgo vejez y con un numero de semanas superior a 1.150,'}]))




    def test_fn_check_lesser_than(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_lesser_than,"TotalSemanas","0")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))




    def test_fn_check_lesser_than_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_lesser_than,"TotalSemanas","0")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'valor en TotalSemanas es mayor que 0,'}]))




    def test_fn_check_afiliado_younger_12(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_afiliado_younger_12)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))



    def test_fn_check_afiliado_younger_12_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'30001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_afiliado_younger_12)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'30001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'afiliado menor a 12 años,'}]))





    def test_fn_check_vejezanticipada_solicitud(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_vejezanticipada_solicitud)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))




    def test_fn_check_vejezanticipada_solicitud_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'19991226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20001212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_vejezanticipada_solicitud)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'19991226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20001212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'edad a la fecha de solicitud es inferior a la debida para hombres,'}]))




    def test_fn_check_multimodal_user(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_multimodal_user)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':''}]))






    def test_fn_check_multimodal_user_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':'yyyyyyyyyyyyyyyy',\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_multimodal_user)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':'yyyyyyyyyyyyyyyy',\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'validacionDetected':'usuario con multiples modalidades de pension,'}]))                                                                      


    def test_fn_check_bigger_than_other(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'llaveMayor':'5',\
                           'llaveMenor':'1',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_bigger_than_other,"llaveMayor","llaveMenor")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'llaveMayor':'5',\
                                    'llaveMenor':'1',\
                                    'validacionDetected':''}]))





    def test_fn_check_bigger_than_other_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'llaveMayor':'1',\
                           'llaveMenor':'5',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_bigger_than_other,"llaveMayor","llaveMenor")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'llaveMayor':'1',\
                                    'llaveMenor':'5',\
                                    'validacionDetected':'valor en llaveMayor es menor que llaveMenor,'}]))




    def test_fn_devoSaldo_lesser_than(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'5',\
                           'CAI':'1',\
                           'Bono':'6',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_devoSaldo_lesser_than)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'5',\
                                    'CAI':'1',\
                                    'Bono':'6',\
                                    'validacionDetected':''}]))




    def test_fn_devoSaldo_lesser_than_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'5',\
                           'CAI':'1',\
                           'Bono':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_devoSaldo_lesser_than)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'5',\
                                    'CAI':'1',\
                                    'Bono':'2',\
                                    'validacionDetected':'devolucion de saldo  es mayor a la suma de bono y saldo CAI,'}]))







    def test_fn_devoSaldo_lesser_than_bono(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_devoSaldo_lesser_than_bono)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'validacionDetected':''}]))




    def test_fn_devoSaldo_lesser_than_bono_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'6',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_devoSaldo_lesser_than_bono)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'6',\
                                    'validacionDetected':'devolucion de saldo  es menor al bono,'}]))




    def test_fn_bono_sin_fecha_reden(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'EstadoBono':'EMI',\
                           'FechaRedencionBono':'2001-05-12',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_bono_sin_fecha_reden)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'EstadoBono':'EMI',\
                                    'FechaRedencionBono':'2001-05-12',\
                                    'validacionDetected':''}]))




    def test_fn_bono_sin_fecha_reden_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'EstadoBono':'EMI',\
                           'FechaRedencionBono':'None',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_bono_sin_fecha_reden)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'EstadoBono':'EMI',\
                                    'FechaRedencionBono':'None',\
                                    'validacionDetected':'Bono con informacion y estado EMI, RED o CUS sin fecha de rendicion,'}]))



    def test_fn_check_pension_less_than_50(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_less_than_50)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'validacionDetected':''}]))




    def test_fn_check_pension_less_than_50_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'10',\
                           'TipoPension':'2',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_pension_less_than_50)
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'10',\
                                    'TipoPension':'2',\
                                    'validacionDetected':'persona con pension de invalidez o sobrevivencia con menos de 50 en campo total_semanas - semanasAlMomentoDeLaDefinicion,'}]))


    def test_fn_check_mesada_pensional_0(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_mesada_pensional_0,"ValorPago")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':''}]))




    def test_fn_check_mesada_pensional_0_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'0',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_mesada_pensional_0,"ValorPago")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'0',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':'valorPago es menor o igual a 0,'}]))



    def test_fn_check_mesada_pensional_below(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_mesada_pensional_below,"ValorPago","1000")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':''}]))



    def test_fn_check_mesada_pensional_below_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10201226',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSolicitud':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_mesada_pensional_below,"ValorPago","2000")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10201226',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSolicitud':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':'valor de mesada pensional es menor a el SMMLV,'}]))


    def test_fn_check_date_compare(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'10210101',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_date_compare,'FechaSiniestro', "%Y%m%d", "FechaNacimiento", "%Y%m%d")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'10210101',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':''}]))


    def test_fn_check_date_compare_wrong(self):
        configsReader = ConfigsReader()
        validador = Validador(configsReader._vConfig)
        dictBeneficiario=[{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                           'fechaDato':'2021-05-28',\
                           'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                           'FechaFinTemporalidad':'20151231',\
                           'ConsecutivoPension':'2',\
                           'SecuenciaBeneficiario':'',\
                           'CalidadBeneficiario':'I',\
                           'FechaNacimiento':'20210101',\
                           'IdentificacionPension':None,\
                           'TipoBeneficiario':'',\
                           'Parentesco':'',\
                           'ModalidadPension':'2',\
                           'PENCON':'24',\
                           'IndicadorPensionGPM':'s',\
                           'SubtipoBeneficiario':'',\
                           'TotalSemanas':'-1000',\
                           'afiliadosDevoluciones':'qqqqqq',\
                           'FechaSiniestro':'20201212',\
                           'Sexo':'MASCULINO',\
                           'ValorPago':'1000',\
                           'fechaNacimientoAfiliado':None,\
                           'tipoDePension':' 1 ',\
                           'CAI':' 1 ',\
                           'prueba':None,\
                           'VejezAnticipada':'VEJEZ ANTICIPADA',\
                           'DevolucionSaldos':'3',\
                           'CAI':'1',\
                           'Bono':'1',\
                           'SemanasMomentoDefinicion':'1000',\
                           'TipoPension':'2',\
                           'TipoPersona':'Cliente',\
                           'validacionDetected':''}]

        with TestPipeline() as p:
            inputt = p | beam.Create(dictBeneficiario)
            output = inputt |  beam.Map( validador.fn_check_date_compare,'FechaSiniestro', "%Y%m%d", "FechaNacimiento", "%Y%m%d")
            assert_that(output,
                        equal_to([{'BeneficiariosID':'cb528078-4493-421e-8cd1-b758dc988952',\
                                    'fechaDato':'2021-05-28',\
                                    'afi_hash64':'+v+C/MyUIFbvRiTf0ncuZpviJEYVA6yH0HnRUJDcz5k=',\
                                    'FechaFinTemporalidad':'20151231',\
                                    'ConsecutivoPension':'2',\
                                    'SecuenciaBeneficiario':'',\
                                    'CalidadBeneficiario':'I',\
                                    'FechaNacimiento':'20210101',\
                                    'IdentificacionPension':None,\
                                    'TipoBeneficiario':'',\
                                    'Parentesco':'',\
                                    'ModalidadPension':'2',\
                                    'PENCON':'24',\
                                    'IndicadorPensionGPM':'s',\
                                    'SubtipoBeneficiario':'',\
                                    'TotalSemanas':'-1000',\
                                    'afiliadosDevoluciones':'qqqqqq',\
                                    'FechaSiniestro':'20201212',\
                                    'Sexo':'MASCULINO',\
                                    'ValorPago':'1000',\
                                    'fechaNacimientoAfiliado':None,\
                                    'tipoDePension':' 1 ',\
                                    'CAI':' 1 ',\
                                    'prueba':None,\
                                    'VejezAnticipada':'VEJEZ ANTICIPADA',\
                                    'DevolucionSaldos':'3',\
                                    'CAI':'1',\
                                    'Bono':'1',\
                                    'SemanasMomentoDefinicion':'1000',\
                                    'TipoPension':'2',\
                                    'TipoPersona':'Cliente',\
                                    'validacionDetected':'FechaNacimiento mayor a FechaSiniestro,'}]))                                                                                                                                                                                    
                                                    
if __name__ == '__main__':
    unittest.main()


    