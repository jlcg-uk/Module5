
import pandas as pd

# Now import the function
from calculatorCopy import Calculator

if __name__ == "__main__":
    
    #PRINT 934*34
    myCalc = Calculator(a=934, b=34)
    answer = myCalc.get_product()
    print(answer)


    #PRINT 3 TIMES TABLE
    # Create an empty list to store numbers
    numbers = []
    # Loop through numbers 1 to 10
    for i in range(1, 11):
        numbers.append(Calculator(i,3).get_product())
    # Create a DataFrame from the list
    df = pd.DataFrame(numbers, columns=['Number'])

    #Print data frame:
    print(df)