class Mapa_Tabelas:
    
    def __init__(self, 
                 data_sample,
                 table_details, 
                 schema='bronze',
                 viz_perc_unicos = 3):
        
        self.__data_sample = data_sample
        self.__table_details = table_details
        self.__schema = schema
        self.__viz_perc_unicos = viz_perc_unicos
        self.__is_fitted = False


    def __mapa_variaveis(self, 
                         __data_sample):

        if self.__is_fitted:
            features = __data_sample.columns.to_list()    

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

                dict_var['Unicos'].append(__data_sample[feature].nunique())
                dict_var['Unicos %'].append(round(__data_sample[feature].nunique()/__data_sample[feature].count(), 4)*100)
                dict_var['column_name'].append(feature)
                dict_var['Tipo'].append(__data_sample[feature].dtype)
                dict_var['Qtde'].append(__data_sample[feature].count())
                dict_var['Nulos %'].append(round(__data_sample[feature].isnull().sum() / __data_sample.shape[0], 4)*100)
                __values = df[feature].value_counts().index

                if (__data_sample[feature].dtype == "O") \
                    or (__data_sample[feature].dtype == 'M8[ns]') \
                    or (__data_sample[feature].dtype == 'datetime64[ns]'):        

                    dict_var['Avg'].append("-")
                    dict_var['Med'].append("-")
                    dict_var['p75'].append("-")
                    dict_var['p99'].append("-")

                    if (__data_sample[feature].dtype != "O"):
                        dict_var['Amostra'].append(__values[:2].strftime('%Y-%m-%d %H:%m').astype(str).str.cat(sep=', '))     
                        dict_var['Min'].append(__values[-1].strftime('%Y-%m-%d %H:%m'))
                        dict_var['Max'].append(__values[0].strftime('%Y-%m-%d %H:%m'))

                    else:
                        dict_var['Amostra'].append(__values[:4].astype(str).str.cat(sep=', '))
                        dict_var['Min'].append(__values[-1])
                        dict_var['Max'].append(__values[0])     

                else:      
                    dict_var['Avg'].append(__data_sample[feature].mean())
                    dict_var['Min'].append(__data_sample[feature].min())
                    dict_var['Med'].append(__data_sample[feature].median())
                    dict_var['p75'].append(np.percentile(__data_sample[pd.notnull(__data_sample[feature])][feature],75))     
                    dict_var['p99'].append(np.percentile(__data_sample[pd.notnull(__data_sample[feature])][feature],99))
                    dict_var['Max'].append(__data_sample[feature].max())
                    dict_var['Amostra'].append(__values[:4].astype(int).astype(str).str.cat(sep=', '))

            self.__data_var = pd.DataFrame.from_dict(data = dict_var)
            
        return self.__data_var
    
        
        
    def __mapa_details(self, 
                      __table_details): 
        
        mp = self.__mapa_variaveis(self.__data_sample)
        data_total = pd.merge(__table_details, mp, how='left', on='column_name')
        
        
        data_total['nullable_old'] = data_total.nullable
        data_total['nullable'] = np.where(data_total['Nulos %']>0.5, 1, 0) 
        data_total['check'] = data_total['nullable'] == data_total['nullable_old']
        
        
        self.data_total = data_total
        
        return self.data_total
    
    
    def exemplo_valores(self):
        
        features = self.__data_sample.columns.to_list()    
        for feature in features:
            real_perc_unicos = round(self.__data_sample[feature].nunique()/self.__data_sample[feature].count(), 4)*100
    
            if real_perc_unicos <= self.__viz_perc_unicos:
                df1 = self.__data_sample[feature].value_counts(normalize=True)*100
                df2 = self.__data_sample[feature].value_counts()
                dfvc = pd.concat([df1, df2], axis=1)
                dfvc.columns = ['perc', 'qtde'] 

                print(feature,'\n', dfvc.head((self.__viz_perc_unicos*10)), '\n', '\n', '\n')        
        
        
    def fit(self):
        
        self.__is_fitted = True
        self.__mapa_details(self.__table_details)
        self.data_document = self.data_total[['column_name', 'data_type_name', 'nullable', 'column_default', 'Amostra']]              
        
        return self.data_total
