# Run sql files via django#
# www.heliosfoundation.org
import os, csv, re
from datetime import datetime
import codecs
import chardet

from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import LabelCommand, BaseCommand
from optparse import make_option
from django.db import models

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

    def check_fkey(self, key, field):
        """ Build fkey mapping via introspection of models """
        #TODO fix to find related field name rather than assume second field
        if field.__class__ == models.ForeignKey:
            key += '(%s|%s)' % (field.related.parent_model.__name__,
                                field.related.parent_model._meta.fields[1].name,)
        return key

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
                    #if fieldmap.has_key(key):
                        #field = fieldmap[key]
                        #key = self.check_fkey(key, field)
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
        for row in self.csvfile[1:]:
            counter += 1

            # create the main instance
            #model_instance = self.model()
            #model_instance.csvimport_id = csvimportid
            instance_tree = {'model': self.model, 'fks': {}, 'm2ms': {}, 'vals':{}}

            for (field_names, column) in self.mappings:
                if self.nameindexes:
                    column = indexes.index(column)
                else:
                    column = int(column)-1
                
                value = row[column]
                
                if self.debug:
                    self.loglist.append('%s.%s = "%s"' % (self.model.__name__, 
                                                          field, value))
                   
                def clean(cell, field_type, loglist):
                    value = cell.strip()
                    if field_type in DATE:
                        #TODO make this more flexible
                        value = datetime.strptime(cell, "%m/%d/%Y")

                    elif field_type in NUMERIC:
                        if not value:
                            value = 0
                        else:
                            try:
                                value = float(value)
                            except:
                                loglist.append('Column %s = %s is not a number so is set to 0' \
                                                    % (field, value))
                                value = 0
                        if field_type in INTEGER:
                            if value > 9223372036854775807:
                                loglist.append('Column %s = %s more than the max integer 9223372036854775807' \
                                                    % (field, value))
                            if str(value).lower() in ('nan', 'inf', '+inf', '-inf'):
                                loglist.append('Column %s = %s is not an integer so is set to 0' \
                                                    % (field, value))
                                value = 0
                            value = int(value)
                            if value < 0 and field_type.startswith('Positive'):
                                loglist.append('Column %s = %s, less than zero so set to 0' \
                                                    % (field, value))
                                value = 0
                    return value
               
                
                current_leaf = instance_tree
                # then for each field...a
                field_names = list(field_names)
                while field_names:
                    field_name = field_names.pop(0)
                    try:
                        field = None
                        for f in current_leaf['model']._meta.fields:
                            if f.name == field_name:
                                field = f
                                break
                        if not field:
                            continue # TODO

                    except: # todo catch the correct exception
                        continue

                    field_type = field.get_internal_type()
                        
                    if field_type == 'ForeignKey': # TODO
                        # TODO: fetch existing models that match paramters
                        if field_name not in current_leaf['fks']:
                            fk_model = field.related.parent_model # TODO
                            #instance = fk_model()
                            current_leaf['fks'][field_name]= {'model': fk_model, 'fks': {}, 'm2ms': {}, 'vals': {}}
                        current_leaf = current_leaf['fks'][field_name]
                    elif field_type == 'ManyToMany':
                        #TODO
                        # will need to pop the next entry because we have a number, too
                        break
                    else:
                        # This is either a regular value field or wrong
                        try:
                            # prepare the value
                            value = clean(row[column], field_type, self.loglist)
                            current_leaf['vals'][field_name] = value
                            break
                        except:
                            pass
                            #try:
                            #    value = model_instance.getattr(field).to_python(value)
                            #except:
                            #    try:
                            #        value = datetime(value)
                            #    except:
                            #        value= None
                            #        self.loglist.append('Column %s failed' % field)

                    #if foreignkey:
                    #    fk_model_type, fk_field = foreignkey
                    #    if not fk_model_type in fk_instances: #self.insert_fkey(foreignkey, row[column])
                    #        fk_model = models.get_model(self.app_label, fk_model_type)
                    #        fk_instances[fk_model_type] = fk_model()
                    #    fk_instances[fk_model_type].__setattr__(field, value)
                    #
                    #    value = fk_instances[fk_model_type]
               
            #for instance in fk_instances.values():
            #    instance.save()

            #if self.defaults:
            #    for (field, default_value, foreignkey) in self.defaults:
            #        try:
            #            done = model_instance.getattr(field)
            #        except:
            #            done = False
            #        if not done:
            #            # TODO add foreign key in a consistent way 
            #            if foreignkey:
            #                default_value = self.insert_fkey(foreignkey, default_value)
            #            model_instance.__setattr__(field, default_value)
            #if self.deduplicate:
            #    matchdict = {}
            #    for (column, field, foreignkey) in self.mappings:
            #        matchdict[field + '__exact'] = getattr(model_instance, 
            #                                               field, None)
            #    try:
            #        self.model.objects.get(**matchdict)
            #        continue
            #    except ObjectDoesNotExist:
            #        pass
            try:
                self.tree_save(instance_tree)
                #model_instance.save()
            except Exception, err:
                self.loglist.append('Exception found... %s Instance %s not saved.' % (err, counter))
        if self.loglist:
            self.props = { 'file_name':self.file_name,
                           'import_user':'cron',
                           'upload_method':'cronjob',
                           'error_log':'\n'.join(self.loglist),
                           'import_date':datetime.now()}
            return self.loglist
    def tree_save(self, leaf):
        
        instance = None
        # Check to see if the instance exists, otherwise, create it
        if False:
            # TODO
            pass
        else:
            try:
                instance = leaf['model']()
            except Exception, err:
                self.loglist.append('Exception found... %s instance not created: %s' % (leaf['model'], err))
        # assign non fks fields to the main instance
        for field_name, value in leaf['vals'].items():
            try:
                instance.__setattr__(field_name, value)
            except Exception, err:
                self.loglist.append('Exception found... %s Instance not saved.' % (err))

        # save fks first as these may be null=False
        for field_name, fk in leaf['fks'].items():
            try:
                fk = self.tree_save(fk)
            except Exception, err:
                self.loglist.append('Couldnt create fk %s for %s: %s.' % (field_name, instance, err))
                continue
            
            try:
                instance.__setattr__(field_name, fk)
            except Exception, err:
                self.loglist.append('Couldnt add  fk %s for %s: %s.' % (field_name, instance, err))
                continue
      
        # Need to save the main instance before setting m2ms
        #try:
        instance.save()
        #except Exception, err:
        #    self.loglist.append('Couldnt save isntance %s: %s.' % (instance, err))
        
        if False:
            # add m2m fields to the main instance
            for field_name, m2m, in leaf['m2ms'].items():
                try:
                    m2m = self.tree_save(m2m)
                except Exception, err:
                    self.loglist.append('Couldnt create m2m %s for %s: %s.' % (field_name, instance, err))
                    continue

                try:
                    instance.__getattribute__(field_name).add(m2m)
                except Exception, err:
                    self.loglist.append('Couldnt add m2m %s to %s : %s.' % (field_name, instance, err))

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

