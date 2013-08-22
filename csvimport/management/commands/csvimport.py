# Run sql files via django#
# www.heliosfoundation.org
import os, csv, re
from datetime import datetime
import codecs
import chardet

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.core.management.base import LabelCommand, BaseCommand
from optparse import make_option
from django.db import models
from django.db.models.fields import FieldDoesNotExist

INTEGER = ['BigIntegerField', 'IntegerField', 'AutoField',
           'PositiveIntegerField', 'PositiveSmallIntegerField']
FLOAT = ['DecimalField', 'FloatField']
DATE = ['DateTimeField', 'DateField']

NUMERIC = INTEGER + FLOAT
# Note if mappings are manually specified they are of the following form ...
# MAPPINGS = "column1=shared_code,column2=org(Organisation|name),column3=description"
# statements = re.compile(r";[ \t]*$", re.M)

def save_csvimport(props=None, instance=None):
    """ To avoid circular imports do saves here """
    try:
        if not instance:
            from csvimport.models import CSVImport
            csvimp = CSVImport()
        if props:
            for key, value in props.items():
                csvimp.__setattr__(key, value)
        csvimp.save()
        return csvimp.id
    except:
        # Running as command line
        print 'Assumed charset = %s\n' % instance.charset
        print '###############################\n'
        for line in instance.loglist:
            if type(line) != type(''):
                for subline in line:
                    print line
                    print
            else:
                print line
                print

class NonUniqueLeafValues(Exception):
    pass

class TreeSaveException(Exception):
    pass

class NoSuchField(Exception):
    pass

class InvalidValue(Exception):
    pass

class InvalidIndex(Exception):
    pass

class InvalidFieldType(Exception):
    pass

class TempModel(object):


    def __init__(self, model, through=None):
        self.model = model
        self.values = {}
        self.fks = {}
        self.m2ms = {}
        self.through = through
        self.instance = None

    def get_model(self):
        return self.model

    def get_fks(self):
        return [(field, fk) for field, fk in self.fks.values()]

    def get_values(self):
        return [(field, val) for field, val in self.values.values()]

    def get_m2ms(self):
        return [(field, m2m_list) for field, m2m_list in self.m2ms.values()]

    def get_unique_fields_dict(self):
        uf_dict = {}
        for field, fk in self.fks.values():
            if field.unique == True:
                uf_dict[field.name + '__pk'] = fk.pk

        for field, value in self.values.values():
            if field.unique == True:
                uf_dict[field.name + '__exact'] = value

        return uf_dict

    def get_required_fields_dict(self):
        rf_dict = {}
        for field, fk in self.fks.values():
            if field.blank == False:
                if not fk.instance:
                    continue
                rf_dict[field.name + '__pk'] = fk.instance.pk

        for field, value in self.values.values():
            if field.blank == False:
                rf_dict[field.name + '__exact'] = value

        return rf_dict

    def get_field(self, field_name):
        try:
            return self.model._meta.get_field(field_name)
        except FieldDoesNotExist:
            msg = 'Model %s has no field %s.' % (self.model.__name__, field_name)
            raise NoSuchField(msg)
    def set_instance(self, instance):
        self.instance = instance

    def get_instance(self):
        return self.instance

    def clean(self, value, field_type):
        if isinstance(value, str):
            value = value.strip()
        if field_type in DATE:
            try:
                value = datetime.strptime(value, "%d/%m/%Y")
            except:
                raise InvalidValue('Null value passed for date')

        elif field_type in NUMERIC:
            if not value:
                value = 0
            else:
                try:
                    value = float(value)
                except ValueError:
                    msg ='Value (%s) not a number' % (value)
                    raise InvalidValue(msg)
            if field_type in INTEGER:

                if value > 9223372036854775807:
                    msg ='Numeric value (%d) more than max allowable integer' % (value)
                    raise InvalidValue(msg)

                if str(value).lower() in ('nan', 'inf', '+inf', '-inf'):
                    msg ='Value (%s) not an integer' % (value)
                    raise InvalidValue(msg)

                value = int(value)
                if value < 0 and field_type.startswith('Positive'):
                    #loglist.append('Column %s = %s, less than zero so set to 0' \
                    #                    % (field, value))
                    value = 0
        return value

    def add_value(self, field_name, value):

        try:
            field = self.get_field(field_name)
        except Exception, e:
            if self.through:
                self.through.add_value(field_name, value)
                return
            else:
                raise e

        field_type = field.get_internal_type()
        value = self.clean(value, field_type)
        self.values[field_name] = (field, value)

    def add_m2m(self, field_name, ind):

        field = self.get_field(field_name)
        if not field.get_internal_type() == 'ManyToManyField':
            raise InvalidFieldType('%s field passed as value' % (field_type))

        try:
            ind = int(ind)
        except ValueError:
            # field following the m2m fielname isnt a number
            # so we cant parse it to the end
            raise InvalidIndex()

        tm = None

        if field_name not in self.m2ms:
           # current_leaf['m2ms'][field_name] = {}
            self.m2ms[field_name] = (field, {})

        if ind not in self.m2ms[field_name][1]:
            m2m_model = field.related.parent_model
            through = None
            # check if we have a custom through model
            if field.rel.through._meta.auto_created == False:
                through = TempModel(field.rel.through)

            tm = TempModel(m2m_model, through=through)
            self.m2ms[field_name][1][ind] = tm
        else:
            tm = self.m2ms[field_name][1][ind]

        return tm

    def add_fk(self, field_name):

        field = self.get_field(field_name)
        if not field.get_internal_type() == 'ForeignKey':
            raise InvalidFieldType('%s field passed as fk' % (field_type))

        tm = None
        if field_name not in self.fks:
            fk_model = field.related.parent_model
            tm  = TempModel(fk_model)
            self.fks[field_name] = (field, tm)
        else:
            tm = self.fks[field_name][1]

        return tm

class Command(LabelCommand):
    """
    Parse and map a CSV resource to a Django model.

    Notice that the doc tests are merely illustrational, and will not run
    as is.
    """

    option_list = BaseCommand.option_list + (
               make_option('--mappings', default='',
                           help='Please provide the file to import from'),
               make_option('--model', default='iisharing.Item',
                           help='Please provide the model to import to'),
               make_option('--charset', default='',
                           help='Force the charset conversion used rather than detect it')
                   )
    help = "Imports a CSV file to a model"


    def __init__(self):
        """ Set default attributes data types """
        super(Command, self).__init__()
        self.props = {}
        self.debug = False
        self.errors = []
        self.loglist = []
        self.mappings = []
        self.defaults = []
        self.app_label = ''
        self.model = ''
        self.file_name = ''
        self.nameindexes = False
        self.deduplicate = True
        self.csvfile = []
        self.charset = ''

    def handle_label(self, label, **options):
        """ Handle the circular reference by passing the nested
            save_csvimport function
        """
        filename = label
        mappings = options.get('mappings', [])
        modelname = options.get('model', 'Item')
        charset = options.get('charset','')
        # show_traceback = options.get('traceback', True)
        self.setup(mappings, modelname, charset, filename)
        if not hasattr(self.model, '_meta'):
            msg = 'Sorry your model could not be found please check app_label.modelname'
            try:
                print msg
            except:
                self.loglist.append(msg)
            return
        errors = self.run()
        if self.props:
            save_csvimport(self.props, self)
        self.loglist.extend(errors)
        return

    def setup(self, mappings, modelname, charset, csvfile='', defaults='',
              uploaded=None, nameindexes=False, deduplicate=True):
        """ Setup up the attributes for running the import """
        self.defaults = self.__mappings(defaults)
        if modelname.find('.') > -1:
            app_label, model = modelname.split('.')
        self.charset = charset
        self.app_label = app_label
        self.model = models.get_model(app_label, model)
        if mappings:
            self.mappings = self.__mappings(mappings)
        self.nameindexes = bool(nameindexes)
        self.file_name = csvfile
        self.deduplicate = deduplicate
        if uploaded:
            self.csvfile = self.__csvfile(uploaded.path)
        else:
            self.check_filesystem(csvfile)

    def check_filesystem(self, csvfile):
        """ Check for files on the file system """
        if os.path.exists(csvfile):
            if os.path.isdir(csvfile):
                self.csvfile = []
                for afile in os.listdir(csvfile):
                    if afile.endswith('.csv'):
                        filepath = os.path.join(csvfile, afile)
                        try:
                            lines = self.__csvfile(filepath)
                            self.csvfile.extend(lines)
                        except:
                            pass
            else:
                self.csvfile = self.__csvfile(csvfile)
        if not getattr(self, 'csvfile', []):
            raise Exception('File %s not found' % csvfile)

    def run(self, logid=0):
        if self.nameindexes:
            indexes = self.csvfile.pop(0)
        counter = 0
        if logid:
            csvimportid = logid
        else:
            csvimportid = 0
        mapping = []
        fieldmap = {}
        for field in self.model._meta.fields:
            fieldmap[field.name] = field

        if self.mappings:
            self.loglist.append('Using manually entered mapping list')
        else:
            for i, heading in enumerate(self.csvfile[0]):
                key = heading.lower()
                if not key:
                    continue

                mapping.append('column%s=%s' % (i+1, key))
            mappingstr = ','.join(mapping)
            if mapping:
                self.loglist.append('Using mapping from first row of CSV file')
                self.mappings = self.__mappings(mappingstr)
        if not self.mappings:
            self.loglist.append('''No fields in the CSV file match %s.%s\n
                                   - you must add a header field name row
                                   to the CSV file or supply a mapping list''' %
                                (self.model._meta.app_label, self.model.__name__))
            return self.loglist

        for row_ind, row in enumerate(self.csvfile[1:]):
            counter += 1
            # create the top level instance
            instance_tree = TempModel(self.model)

            for (field_names, column) in self.mappings:

                if self.nameindexes:
                    column = indexes.index(column)
                else:
                    column = int(column)-1

                value = row[column]
                if value == '':
                    continue

                if self.debug:
                    self.loglist.append('%s.%s = "%s"' % (self.model.__name__,
                                                          field, value))

                current_leaf = instance_tree

                field_names = list(field_names)
                while field_names:
                    # take the leftmost fieldname and try and resolve
                    # it against the current model
                    field_name = field_names.pop(0)
                    try:
                        try:
                            ind_string = field_names.pop(0)
                            try:
                                ind = int(ind_string)
                            except ValueError:
                                # not an ind; put it back
                                # this should be an fk
                                field_names = [ind_string] + field_names
                                current_leaf = current_leaf.add_fk(field_name)
                                continue
                            current_leaf = current_leaf.add_m2m(field_name, ind)
                            continue

                        except IndexError:
                            # Last field name, this is just a regular
                            # value field
                            try:
                                current_leaf = current_leaf.add_value(field_name, value)
                            except InvalidValue, e:
                                msg = "Could not prepare value '%s' in cell [%s, %s]" % \
                                    (value, row_ind, column)
                                self.loglist.append(msg)
                    except InvalidFieldType, e:
                        msg = "mapping string mapped field %s to invalid field type (%s)" % \
                            (field_name, e)
                        self.loglist.append(msg)

                    except NoSuchField, e:
                        msg = "%s" % (e)
                        self.loglist.append(msg)

            try:
                instance = self.tree_save(instance_tree)

                # TODO: this is a hangover from the original code; check if necessary
                instance.csvimport_id = csvimportid
                instance.save()
            except TreeSaveException, err:
                self.loglist.append('Instance %s not saved (%s)' % (counter, err))
        if self.loglist:
            self.props = { 'file_name':self.file_name,
                           'import_user':'cron',
                           'upload_method':'cronjob',
                           'error_log':'\n'.join(self.loglist),
                           'import_date':datetime.now()}
            return self.loglist

    def fetch_for_values(self, leaf):

        # Match always on unique fields
        # Failing that, try required fields?
        # Never match on optional fields
        # (save an instance, and then
        # add a new optional field and it
        # wont match)

        matchdict = {}
        instance = None
        matchdict = leaf.get_unique_fields_dict()

        if not len(matchdict):
            # no unique values specified
            # add required values
            matchdict = leaf.get_required_fields_dict()

        if not len(matchdict):
            # No values specified. No point in searching
            return instance

        # Note: skip M2M fields as they don't really 'identify' their parent

        try:
            instance = leaf.get_model().objects.get(**matchdict)
        except MultipleObjectsReturned:
            # The leaf values matched multiple instances.
            # No clear path ahead here so bail
            raise NonUniqueLeafValues()

        except ObjectDoesNotExist:
            # this doesn't matter; we'll just create it later
            pass

        return instance

    def tree_save(self, leaf):

        # save fks first as these may be null=False
        for field, fk in leaf.get_fks():
            try:
                self.tree_save(fk)
            except TreeSaveException, e:
                self.loglist.append('Couldnt create fk %s for %s: %s.'
                        % (field.name, fk.get_model(), e))
                continue

        try:
            instance = self.fetch_for_values(leaf)
        except NonUniqueLeafValues:
            error = 'values (%s) yeilded multiple instances for model %s' % (
                ', '.join(['%s:%s' % (field.name, value) for field, value in leaf.get_values()]),
                leaf.get_model())

            raise TreeSaveException(error)

        if not instance:
            try:
                instance = leaf.get_model()()
            except Exception, err:
                error = '%s instance not created: %s' % (leaf.get_model(), err)
                raise TreeSaveException(error)

        # assign non fks fields to the main instance
        for field, value in leaf.get_values():
            try:
                instance.__setattr__(field.name, value)
            except Exception, err:
                self.loglist.append('%s Field %s not set for instance %s.' % \
                        (err, field.name, instance))

        # save fks first as these may be null=False
        for field, fk in leaf.get_fks():
            fk = fk.get_instance()
            if not fk:
                continue

            try:
                instance.__setattr__(field.name, fk)
            except Exception, err: # TODO catch explicit exceptions
                self.loglist.append('Couldnt add fk %s to %s: %s.' % \
                        (field.name, fk.get_model(), err))

        # Need to save the main instance before setting m2ms
        try:
            instance.save()
        except Exception, err:
            raise TreeSaveException('main instance save failed: %s' % (err))

        leaf.set_instance(instance)

        # add m2m fields to the main instance
        for field, m2m_list in leaf.get_m2ms():
            for ind, m2m in m2m_list.items():
                try:
                    m2m_instance = self.tree_save(m2m)
                except TreeSaveException, err:
                    self.loglist.append('Couldnt save m2m %s[%s] for %s: %s.' % \
                            (field.name, ind, instance, err))
                    continue
                has_custom_through = field.rel.through._meta.auto_created == False
                if has_custom_through:
                    parent_field_name = field._get_m2m_attr(field.related, 'name')
                    child_field_name = field._get_m2m_reverse_attr(field.related, 'name')
                    m2m.through.add_value(parent_field_name, instance)
                    m2m.through.add_value(child_field_name, m2m.instance)
                    self.tree_save(m2m.through)
                else:
                    try:
                        instance.__getattribute__(field.name).add(m2m_instance)
                    except Exception, err:
                        self.loglist.append('Couldnt add m2m %s to %s : %s.' % (field.name, instance, err))

        return instance



    def insert_fkey(self, foreignkey, rowcol):
        """ Add fkey if not present
            If there is corresponding data in the model already,
            we do not need to add more, since we are dealing with
            foreign keys, therefore foreign data
        """
        fk_key, fk_field = foreignkey
        if fk_key and fk_field:
            fk_model = models.get_model(self.app_label, fk_key)
            matches = fk_model.objects.filter(**{fk_field+'__exact':
                                                 rowcol})

            if not matches:
                key = fk_model()
                key.__setattr__(fk_field, rowcol)
                key.save()

            rowcol = fk_model.objects.filter(**{fk_field+'__exact': rowcol})[0]
        return rowcol

    def error(self, message, type=1):
        """
        Types:
            0. A fatal error. The most drastic one. Will quit the program.
            1. A notice. Some minor thing is in disorder.
        """

        types = (
            ('Fatal error', FatalError),
            ('Notice', None),
        )

        self.errors.append((message, type))

        if type == 0:
            # There is nothing to do. We have to quite at this point
            raise types[0][1], message
        elif self.debug == True:
            print "%s: %s" % (types[type][0], message)

    def __csvfile(self, datafile):
        """ Detect file encoding and open appropriately """
        filehandle = open(datafile)
        if not self.charset:
            diagnose = chardet.detect(filehandle.read())
            self.charset = diagnose['encoding']
        try:
            csvfile = codecs.open(datafile, 'r', self.charset)
        except IOError:
            self.error('Could not open specified csv file, %s, or it does not exist' % datafile, 0)
        else:
            # CSV Reader returns an iterable, but as we possibly need to
            # perform list commands and since list is an acceptable iterable,
            # we'll just transform it.
            return list(self.charset_csv_reader(csv_data=csvfile,
                                                charset=self.charset))

    def charset_csv_reader(self, csv_data, dialect=csv.excel,
                           charset='utf-8', **kwargs):
        csv_reader = csv.reader(self.charset_encoder(csv_data, charset),
                                dialect=dialect, **kwargs)
        for row in csv_reader:
            # decode charset back to Unicode, cell by cell:
            yield [unicode(cell, charset) for cell in row]

    def charset_encoder(self, csv_data, charset='utf-8'):
        for line in csv_data:
            yield line.encode(charset)

    def __mappings(self, mapping_string):
        """
        Parse the mappings, and return a list of them.
        """
        if not mapping_string:
            return []

        model = self.model

        mapping_string = mapping_string.replace(',', ' ')
        mapping_string = mapping_string.replace('column', '')

        """
        Parse the custom mapping syntax (column1=[fk1.fk2...fk3].field,
        etc.)

        """

        pattern = re.compile(r'(\w+)=([\w.]+)')
        matches = pattern.findall(mapping_string)
        matches = list(matches)

        mappings = []
        for mapping in matches:
            column, field_list = mapping
            fields = field_list.split('.')
            mappings.append((fields, column))
            #mappings[ind][2] = parse_foreignkey(mapping[2])
            #mappings[ind] = tuple(mappings[ind])
        return mappings

class FatalError(Exception):
    """
    Something really bad happened.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

