# grey_literature_scraper
A web-scraper for grey literature that accepts a search string and exports the extrated data in a csv and xlsx file.
By: Wiliane Souza

Python version: 3.7.9

-------- Libs you'll need -----
	$ pip install pandas
	$ pip install xlsxwriter
	$ pip install beautifulsoup4
	$ pip install requests
	$ pip install -U selenium
  
Obs.: You will also need to install chromedriver
and maybe replace the executable in the folder with yours.



--- When running the MAIN file, it will ask for a string, 
make sure you put the corret syntax:

*Valid operators are: OR and AND
*Order of precedence: AND, then OR (use parentheses to change it)
*put your keywords in quotes, e.g.: "Apple", "Apple Sauce"
*Don't forget the spaces between your expressions:
  **Wrong -> ("Apple"OR"Apple Souce)AND"Mashed Potatoes"
  **Right -> ("Apple" OR "Apple Souce) AND "Mashed Potatoes"
  
 Obs.: It does not accept strings with parentheses in parentheses yet
      e.g.: (("Apple" OR "Pie") AND "Apple Souce) OR "Honey"
      you can use instead: ("Apple" OR "Pie") AND "Apple Souce OR "Honey"
 
 Using a wrong syntax may cause errors or wrong results.
