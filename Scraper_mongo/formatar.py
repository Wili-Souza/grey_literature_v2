import xlsxwriter

def formatar_excel(writer, website):
    #Pegando dados a partir do escritor para trabalhar com xlsxwriter
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    # Criando formatações ---
    if website == 0:
        link_vcenter = workbook.add_format({'color': 'blue', 
                                            'underline': True, 
                                            'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        text_vcenter = workbook.add_format({'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        v_align = workbook.add_format({'valign' : 'vcenter',
                                        'align' : 'center'})
        
        v_aling_yellow = workbook.add_format({'bg_color': '#ffffbf',
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        text_vcenter_yellow = workbook.add_format({'bg_color': '#ffffbf',
                                            'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        #Formatando ----
        worksheet.set_column('B:B', 15, v_align)
        worksheet.set_column('C:C', 30, text_vcenter_yellow)
        worksheet.set_column('D:D', 60, link_vcenter)
        worksheet.set_column('E:E', 10, v_aling_yellow)
        worksheet.set_column('F:F', 100, text_vcenter)

    elif website == 1 or website == 2:
        link_vcenter = workbook.add_format({'color': 'blue', 
                                            'underline': True, 
                                            'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        text_vcenter = workbook.add_format({'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        v_align = workbook.add_format({'text_wrap': True,
                                        'valign' : 'vcenter',
                                        'align' : 'center'})
        
        v_aling_yellow = workbook.add_format({'text_wrap': True,
                                            'bg_color': '#ffffbf',
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        text_vcenter_yellow = workbook.add_format({'bg_color': '#ffffbf',
                                            'text_wrap': True,
                                            'valign' : 'vcenter',
                                            'align' : 'center'})

        #Formatando ----
        worksheet.set_column('B:B', 20, v_align)
        worksheet.set_column('C:C', 40, text_vcenter_yellow)

        if website == 1:
            worksheet.set_column('D:D', 70, link_vcenter)
        else: 
            worksheet.set_column('D:D', 70, text_vcenter)

        worksheet.set_column('E:E', 20, v_aling_yellow)
        worksheet.set_column('F:F', 10, text_vcenter)
        worksheet.set_column('G:G', 100, text_vcenter)

    #Retornando writer com dados já formatados --- 
    return writer

