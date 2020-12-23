from sqlalchemy import text
from ftp_downloader import ResourceNotFoundError
import traceback
import math

MAX_VALUES_SIZE = 16777215
SIG_FIGS = 3

class ValueFieldTooLongError(Exception):
    pass


def round_n_sig_figs(n, value):
    round(value, n - int(math.floor(math.log10(abs(value)))) - 1)

def round_n_sig_figs_str(n, value):
    try:
        value_f = float(value)
    except ValueError as e:
        print("error value: %s" % value)
    rounded = round_n_sig_figs(n, value_f)
    rounded_s = str(rounded)
    return rounded_s

#just store raw values in case want to do more with them later (apply sample control analysis, etc)
#table needs: (gene_id, gpl, gse, gsm, ref_id, value)
#note there may be multiple values of for the same gene for a given sample (accession series etc), these should typically be averaged out, provide ref_id to differentiate and provide primary key, and also provide back reference if needed for future analysis
# primary key (gene_id, gse, gsm, ref_id)
# note gsm can be in multiple gses, one gpl per gsm, gse can cover multiple gpls (assume this means that gpls must have same set of rows)
#note can use group by and avg to aggregate values in queries (e.g group by gsm and take average value for given gene_id)

#maybe also second table with pre-computed info for fast access, log_2(ratio), -log_10(p) 
#table, (gene_id, gse, gsm, log_2_rat, neg_log_10_p)

#change return from boolean to (boolean include, boolean continue)

#ids are a mapping of row_ids to gene_ids
def handle_gse_gpl(connector, ftp_handler, gse, gpl, ids, db_retry, ftp_retry):
    db_data = []
    #reraise any errors with traceback as error message
    try:
        row_ids = set(ids.keys())
        header = None
        values_map = []

        #super fast check
        def check_include_continue(row):
            nonlocal row_ids
            nonlocal header
            nonlocal values_map

            include_continue = (True, True)

            if header is None:
                header = row
                #check that there are samples (first column is row ids)
                if len(row) < 2:
                    include_continue = (False, False)
                else:
                    #make gsms lowercase
                    for i in range(1, len(header)):
                        #convert gsms to lowercase and strip quotes
                        header[i] = header[i].lower().strip('"')
                        values_map.append({})
                    #don't add header to rows to send to handler, continue
                    include_continue = (False, True)
            else:
                # got all expected rows, terminate
                if len(row_ids) == 0:
                    include_continue = (False, False)
                #ID_REF is quoted, strip quotes
                row[0] = row[0].strip('"')
                row_id = row[0]
                if row_id in row_ids:
                    row_ids.remove(row_id)
                else:
                    include_continue = (False, True)
            return include_continue
        
        def handle_row(row):
            nonlocal values_map
            row_id = row[0]
            gene_id = ids[row_id]

            for i in range(1, len(row)):
                gsm = header[i]
                gsm_val = row[i]
                gsm_val = round_n_sig_figs_str(SIG_FIGS, gsm_val)
                #minus one due to ref_id col offset
                vals = values_map[i - 1].get(gene_id)
                if vals is None:
                    vals = []
                    values_map[i - 1][gene_id] = vals
                vals.append(gsm_val)

        #let callee handle other errors
        try:
            ftp_handler.process_gse_data(gse, gpl, check_include_continue, handle_row, ftp_retry)
        #if a resource not found error was raised then the resource doesn't exist on the ftp server, just skip this one
        except ResourceNotFoundError:
            pass
        else:
            #aggregate results
            for i in range(1, len(header)):
                gsm = header[i]
                data = values_map[i - 1]
                for gene_id in data:
                    vals = data[gene_id]
                    #use bar separated values list
                    val_list_string = "|".join(vals)
                    #make sure value string length doesn't exceed column size (if this happens might have to rework something)
                    if len(val_list_string) > MAX_VALUES_SIZE:
                        raise ValueFieldTooLongError("Value field exceeded %d character limit, length: %d." % (MAX_VALUES_SIZE, len(val_list_string)))
                    fields = {
                        "gsm": gsm,
                        "gene_id": gene_id,
                        "values": val_list_string
                    }
                    db_data.append(fields)
    except Exception as e:
        trace = traceback.format_exc()
        raise RuntimeError(trace)
    return db_data