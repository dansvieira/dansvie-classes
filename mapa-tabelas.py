class Mapa_Tabelas:

    """
    Parameters
    ----------
    data_sample:  
        Resultado da query com os exemplos de dados.
    table_details: 
        Resultado da query com dados sobre as colunas como: 'data_type_name', 'nullable', 'column_default', 'repeatable' 
        que estão setadas no banco de dados.
                
    """    
    
    
    def __init__(self, 
                 data_sample,
                 table_details, 
                 schema='bronze',
                 viz_perc_unicos = 3):
        
        self.data_sample = data_sample
        self.table_details = table_details
        self.schema = schema
        self.viz_perc_unicos = viz_perc_unicos
        self.__is_fitted = False


    def __mapa_variaveis(self, 
                         data_sample):

        if self.__is_fitted:
            features = data_sample.columns.to_list()    

            dict_var = {"column_name": [],
                        "Tipo": [],
                        "Qtde": [],
                        "Nulos %": [],
                        "Unicos": [],
                        "Unicos %": [],
                        "Avg": [],
                        "Min": [],
                        "Med": [],
                        "p75": [],
                        "p99": [],
                        "Max": [], 
                        'Amostra': []}  

            for feature in features:

                dict_var['Unicos'].append(data_sample[feature].nunique())
                dict_var['Unicos %'].append(round(data_sample[feature].nunique()/data_sample[feature].count(), 4)*100)
                dict_var['column_name'].append(feature)
                dict_var['Tipo'].append(data_sample[feature].dtype)
                dict_var['Qtde'].append(data_sample[feature].count())
                dict_var['Nulos %'].append(round(data_sample[feature].isnull().sum() / data_sample.shape[0], 4)*100)
                __values = df[feature].value_counts().index
                
                
                if (data_sample[feature].dtype == "O") \
                    or (data_sample[feature].dtype == 'M8[ns]') \
                    or (data_sample[feature].dtype == 'datetime64[ns]'):        

                    dict_var['Avg'].append("-")
                    dict_var['Med'].append("-")
                    dict_var['p75'].append("-")
                    dict_var['p99'].append("-")

                    if (data_sample[feature].dtype != "O"):
                        dict_var['Amostra'].append(__values[:2].strftime('%Y-%m-%d %H:%m').astype(str).str.cat(sep=', '))     
                        dict_var['Min'].append(__values[-1].strftime('%Y-%m-%d %H:%m'))
                        dict_var['Max'].append(__values[0].strftime('%Y-%m-%d %H:%m'))

                    else:
                        dict_var['Amostra'].append(__values[:4].astype(str).str.cat(sep=', '))
                        try:
                            dict_var['Min'].append(__values[-1])
                            dict_var['Max'].append(__values[0])                                
                        except: 
                            dict_var['Min'].append(" ")
                            dict_var['Max'].append(" ")                        
 

                else:      
                    dict_var['Avg'].append(data_sample[feature].mean())
                    dict_var['Min'].append(data_sample[feature].min())
                    dict_var['Med'].append(data_sample[feature].median())
                    dict_var['p75'].append(np.percentile(data_sample[pd.notnull(data_sample[feature])][feature],75))     
                    dict_var['p99'].append(np.percentile(data_sample[pd.notnull(data_sample[feature])][feature],99))
                    dict_var['Max'].append(data_sample[feature].max())
                    dict_var['Amostra'].append(__values[:4].astype(int).astype(str).str.cat(sep=', '))

            self.__data_var = pd.DataFrame.from_dict(data = dict_var)
            
        return self.__data_var
    
        
        
    def __mapa_details(self, 
                      table_details): 
        
        mp = self.__mapa_variaveis(self.data_sample)
        data_total = pd.merge(table_details, mp, how='left', on='column_name')
        
        
        data_total['nullable_old'] = data_total.nullable
        data_total['nullable'] = np.where(data_total['Nulos %']>0.5, 1, 0) 
        data_total['repeatable'] = np.where(data_total['Unicos %']<99.9, 1, 0) 
        self.data_total = data_total
        
        return self.data_total
    
    
    def exemplo_valores(self):
        
        features = self.data_sample.columns.to_list()    
        for feature in features:
            real_perc_unicos = round(self.data_sample[feature].nunique()/self.data_sample[feature].count(), 4)*100
    
            if real_perc_unicos <= self.viz_perc_unicos:
                df1 = self.data_sample[feature].value_counts(normalize=True)*100
                df2 = self.data_sample[feature].value_counts()
                dfvc = pd.concat([df1, df2], axis=1)
                dfvc.columns = ['perc', 'qtde'] 

                print(feature,'\n', dfvc.head((self.viz_perc_unicos*10)), '\n', '\n', '\n')        
        
        
    def fit(self):
        
        self.__is_fitted = True
        self.__mapa_details(self.table_details)
        
        # arruma a tabela final doc 
        data_document = self.data_total[['column_name', 'data_type_name', 'nullable', 'column_default', 'Amostra', 'repeatable']] 
        data_document.columns = ['column name', 'data type', 'nullable', 'default value', 'sample', 'repeatable']
        data_document['desc type'] = 'Dimensão'
        data_document['description'] = '-'
        data_document['source'] = '-'
        self.data_document = data_document[['column name','source', 'data type', 'desc type', 'nullable', 'repeatable', 'default value','description', 'sample']]                                              
              
        
        return self.data_total
