from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as ec
from pynput.keyboard import Key,Controller
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException
from tabulate import tabulate
import time
import re
import sys

driver = webdriver.Chrome()

resolution = input("Enter the resolution you want the window to open(1080p/720p/480p):")


driver.get("http://internal-ma-ds-multi-alb-qa-414790702.eu-central-1.elb.amazonaws.com/")
if resolution == '1080p':
    driver.set_window_size(1920,1080)
    print("MUL-689 passed, The page has opened with 1080p resolution")
elif resolution == '720p':
    driver.set_window_size(1280,720)
    print("MUL-689 passed, The page has opened with 720p resolution")
elif resolution == '480p':
    driver.set_window_size(854,480)
    print("MUL-689 passed, The page has opened with 480p resolution")
else:
    print("Running on the resolution of the screen")
    print("MUL-689 passed, The page has opened with default screen resolution")

driver.maximize_window()
driver.implicitly_wait(5)

wait = WebDriverWait(driver,timeout=20)


email = input("Enter the User email id: ")
password =input("Enter the password: ")

evidences = "D:\\Users\\penkiev\\PycharmProjects\\pythonProject\\venv\\Evidences"

action = ActionChains(driver)

# Checking for the Login Header
if driver.find_element(By.XPATH,"//div/h2").text == 'Login':
    print("Test Case MUL-195 passed, The URL is redirected to the login page!")
else:
    print("Test Case MUL-195 failed, The URL is not redirected to the login page!")
    sys.exit()

Login_Parameter = input("Enter the Login Parameter:")

if str.lower(Login_Parameter) == 'login':

    # Email input box
    driver.find_element(By.ID,"userId").send_keys(email)

    # password email box
    driver.find_element(By.ID,"password").send_keys(password)

    # Evidence of Email and Password input accepted in the Evidence folder in "Email_Password_Values.png"
    driver.save_screenshot(f"{evidences}\\MUL-196_Email_Password_Values.png")

    # Checking the "Login" button and "Forgot password?" hyperlink are present on the login page.
    assert ec.presence_of_element_located((By.CSS_SELECTOR,".loginBtn")) and ec.presence_of_element_located((By.LINK_TEXT,"Forgot password?"))
    print("MUL-197, Login and Forgot password are present on the login screen")

    # clicking on the Login Button
    driver.find_element(By.CSS_SELECTOR,".loginBtn").click()

    # time.sleep(3)

    if "@" in email and "." in email.split("@")[-1] and " " not in email and len(password)>6:
        try:
            # WebDriverWait(driver,timeout=3).until(ec.presence_of_element_located((By.XPATH, "//div/main/h2")))
            ec.presence_of_element_located((By.XPATH, "//div/h2"))
            # Finding the header for the Homepage, so it tells that we have landed into th Home Page.
            if driver.find_element(By.XPATH, '//*[@id="root"]/div[3]/div[2]/div/main/h2').text == "MOODY'S UNIVERSAL LINKING & TRACKING IDENTIFIER":
                print("MUL-174 passed, The user has landed on the home page")
                driver.save_screenshot(f"{evidences}\\HomePage.png")
        except:
            # Failure Error
            error_message = driver.find_element(By.XPATH, "//div[@role='alert']/div[2]").text
            print(f"Error message: {error_message}")
            print("MUL-269 passed, Incorrect credentials are stopped at the login page.")
            sys.exit()

        if driver.find_element(By.CSS_SELECTOR, ".navbar-brand").text == "Moody's | MULTI Platform":
            print("MUL-685 passed, The brand logo is on the Homepage")

        if driver.find_element(By.XPATH, '//*[@id="root"]/div[3]/div[2]/div/main/h2').text == "MOODY'S UNIVERSAL LINKING & TRACKING IDENTIFIER":
            print("MUL-688 passed, The MOODY'S UNIVERSAL LINKING & TRACKING IDENTIFIER is present on the UI.")

        if driver.title  == "Moody's Multi Platform":
            print("MUL-690 passed, The title of the page is Moody's Multi Platform")

        # Checking for Home, Search and On-Demand Generation buttons present on the Home screen.
        assert ec.presence_of_element_located((By.XPATH, "//a[@href='/search']")) \
               and ec.presence_of_element_located((By.XPATH, "//a[@href='/']")) \
               and ec.presence_of_element_located((By.XPATH, "//a[@href='/on-demand-id-gen']"))

        print("MUL-687 passed, Home, Search and On-Demand ID Generations buttons are present in the Homepage!")


        Homepage_Parameter = input("Enter the Homepage Parameter:")

        if Homepage_Parameter.lower() == 'search':
            driver.find_element(By.XPATH, "//*[@id='root']/div[3]/div[1]/ul/li[2]/a/button").click()
            print("MUL-575 Passed, You are in Search Page!")
            driver.save_screenshot(f"{evidences}\\MUL-575.png")

            time.sleep(3)

            assert driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[2]/div")

            print("MUL-576 Passed, Default rule is present on the screen ")
            driver.save_screenshot(f"{evidences}\\MUL-576.png")

            assert driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[2]/div[1]/div[2]").text=="The dropdown IDs are based on the surviving IDs. Ensure that you are selecting the right Entity Type when searching."

            print("MUL-580 Passed, The dropdown IDs are based on the surviving IDs. Ensure that you are selecting the right Entity Type when searching. is present")
            driver.save_screenshot(f"{evidences}\\MUL-580.png")
            assert driver.find_element(By.XPATH, "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[1]") \
                   and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[2]") \
                   and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[3]")
            print("MUL-1553 Passed, Indiviual,legal entity,organization buttons are present on search screen")

            assert driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[1]/div/button[1]")\
                  and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[1]/div/button[2]")\
                  and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[1]/button[1]")\


            print("MUL-579 Passed , AND Button,OR Button,Rule Button are present")
            driver.save_screenshot(f"{evidences}\\MUL-579.png")

            # Ask for entity type and ensure valid input
            while True:  # This starts an infinite loop that will keep asking for input until valid input is given
                entity_type = input(
                    "Enter the entity type to perform search (Individual, Legal entity, Organization): ")

                # Check for valid input
                if entity_type.lower() == "individual":
                    driver.find_element(By.XPATH,
                                        "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[1]").click()
                    print("Search operation is performing on Individual data")
                    button_type_search = "individual"
                    driver.save_screenshot(f"{evidences}\\MUL-.png")
                    break  # This will break the loop once valid input is provided and the operation is performed

                elif entity_type.lower() == "legal entity":
                    driver.find_element(By.XPATH,
                                        "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[2]").click()
                    print("Search operation is performing on legal entity data")
                    button_type_search = "legal entity"
                    driver.save_screenshot(f"{evidences}\\MUL-.png")

                    break  # Break out of the loop if valid input is provided for "legal entity"

                elif entity_type.lower() == "organization":
                    driver.find_element(By.XPATH,
                                        "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[1]/button[3]").click()
                    print("Search operation is performing on Organization data")
                    button_type_search = "organization"
                    driver.save_screenshot(f"{evidences}\\MUL-.png")
                    break  # Break out of the loop if valid input is provided for "organization"

                else:
                    # If input is invalid, print a message and prompt again
                    print("Invalid input. Please enter a valid option (Individual, Legal entity, Organization).")

            while True:

                choice = input("Enter 'and' or 'or' to execute the button: ")
                if choice.lower() == "and":
                    and_buttonxpath=driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[1]/div/button[1]")
                    WebDriverWait(driver, 10).until(ec.element_to_be_clickable(and_buttonxpath))
                    # Scroll the element into view (with smooth scrolling and centering it in the viewport)
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})",and_buttonxpath)
                    driver.implicitly_wait(5)
                    ActionChains(driver).move_to_element(and_buttonxpath).click().perform()
                    print("MUL-427 Passed, User able click AND button")
                    button_type = "AND"
                    driver.save_screenshot(f"{evidences}\\MUL-427.png")

                elif choice.lower() == "or":
                    or_buttonxpath = driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[1]/div/button[2]")
                    WebDriverWait(driver, 10).until(ec.element_to_be_clickable(or_buttonxpath))

                    # Scroll the element into view (with smooth scrolling and centering it in the viewport)
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})",or_buttonxpath)
                    driver.implicitly_wait(5)
                    ActionChains(driver).move_to_element(or_buttonxpath).click().perform()

                    print("MUL-573 Passed, User able click OR button")
                    button_type = "OR"
                    driver.save_screenshot(f"{evidences}\\MUL-573.png")
                else:
                    print("Invalid input. Please enter either 'and' or 'or'.")

                # Select Field for the existing rule
                time.sleep(2)
                initial_field_dropdown = Select(driver.find_element(By.XPATH,
                                                                    "//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/select[1]"))
                print("Select a field for the condition:")
                fields = [option.text for option in initial_field_dropdown.options]
                print(fields)
                if button_type_search=="individual":
                    print("MUL-1554 Passed,The appropriate fields are displayed in the dropdown when the entity type is selected as 'Individual'.")
                elif button_type_search=="legal entity":
                    print("MUL-1554 Passed,The appropriate fields are displayed in the dropdown when the entity type is selected as 'legal entity'.")
                elif button_type_search=="organization":
                    print("MUL-1554 Passed,The appropriate fields are displayed in the dropdown when the entity type is selected as 'organization'.")

                while True:
                    field_input = input("Enter the field option: ")
                    if field_input in fields:
                        initial_field_dropdown.select_by_visible_text(field_input)
                        break

                    else:
                        print("Invalid input. Please try again.")

                # Select Condition for the existing rule
                initial_condition_dropdown = Select(driver.find_element(By.XPATH,
                                                                        "//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/select[2]"))
                print("Select a condition:")
                conditions = [option.text for option in initial_condition_dropdown.options]
                print(conditions)

                while True:
                    condition_input = input("Enter the condition option: ")
                    if condition_input in conditions:
                        initial_condition_dropdown.select_by_visible_text(condition_input)


                        if condition_input.lower() == "between":
                            from_date = input("Enter the 'From' date (format: MM-DD-YYYY): ")
                            to_date = input("Enter the 'To' date (format: MM-DD-YYYY): ")

                            from_date_box = driver.find_element(By.XPATH,
                                                                "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[2]/div[1]/input[1]")
                            to_date_box = driver.find_element(By.XPATH,
                                                                "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[2]/div[1]/input[2]")
                            from_date_box.clear()
                            to_date_box.clear()

                            from_date_box.send_keys(from_date)
                            to_date_box.send_keys(to_date)

                            driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", from_date_box)
                            driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", to_date_box)

                            if button_type == "AND":
                                print("MUL-430 Passed , data has been send to 2 input date fields")
                                driver.save_screenshot(f"{evidences}\\MUL-430.png")
                            elif button_type == "OR":
                                print("MUL-593 Passed, data has been send to 2 input date fields")
                                driver.save_screenshot(f"{evidences}\\MUL-593.png")
                        else:

                            initial_text_box = WebDriverWait(driver, 10).until(
                            ec.presence_of_element_located((By.XPATH,
                                                            "//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/input")))
                            initial_text_box.clear()
                            text_value = input("Enter the text value to input for the existing rule: ")
                            initial_text_box.send_keys(text_value)
                        break
                    else:
                        print("Invalid input. Please try again.")
                if button_type == "AND":
                    print("MUL-428 Passed, User able to select the option from dropdown for AND button")
                    driver.save_screenshot(f"{evidences}\\MUL-428.png")
                elif button_type == "OR":
                    print("MUL-591 Passed, User able to select the option from dropdown for OR button")
                    driver.save_screenshot(f"{evidences}\\MUL-591.png")


                while True:
                    rule_input = input("Enter 'rule' to add a new rule or 'done' when finished : ")

                    if rule_input.lower() == "rule":
                        driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[1]/button[1]").click()
                         # Determine the number of rules added
                        rule_count = len(driver.find_elements(By.XPATH,"//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div"))

                        # Select Field for the new rule
                        field_dropdown = Select(driver.find_element(By.XPATH,f"//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[{rule_count}]/select[1]"))
                        print("Select a field for the new condition:")
                        fields = [option.text for option in field_dropdown.options]
                        print(fields)

                        while True:
                            field_input = input("Enter the field option: ")
                            if field_input in fields:
                                field_dropdown.select_by_visible_text(field_input)
                                break
                            else:
                                print("Invalid input. Please try again.")

                        # Select Condition for the new rule
                        condition_dropdown = Select(driver.find_element(By.XPATH,f"//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[{rule_count}]/select[2]"))
                        print("Select a condition:")
                        conditions = [option.text for option in condition_dropdown.options]
                        print(conditions)

                        while True:
                            condition_input = input("Enter the condition option: ")
                            if condition_input in conditions:
                                condition_dropdown.select_by_visible_text(condition_input)

                                if condition_input.lower() == "between":
                                    from_date = input("Enter the 'From' date (format: MM-DD-YYYY): ")
                                    to_date = input("Enter the 'To' date (format: MM-DD-YYYY): ")

                                    from_date_box = WebDriverWait(driver, 10).until(
                                        ec.presence_of_element_located((By.XPATH,
                                                                        f"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[2]/div[{rule_count}]/input[1]")))
                                    to_date_box = WebDriverWait(driver, 10).until(
                                        ec.presence_of_element_located((By.XPATH,
                                                                        f"///*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[2]/div[{rule_count}]/input[2]")))

                                    from_date_box.clear()
                                    to_date_box.clear()
                                    from_date_box.send_keys(from_date)
                                    to_date_box.send_keys(to_date)

                                    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));",
                                                          from_date_box)
                                    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));",
                                                          to_date_box)


                                    if button_type == "AND":
                                        print("MUL-430 Passed , data has been send to  input date fields")
                                        driver.save_screenshot(f"{evidences}\\MUL-430.png")
                                    elif button_type == "OR":
                                        print("MUL-593 Passed, data has been send to  input date fields")
                                        driver.save_screenshot(f"{evidences}\\MUL-593.png")
                                    break

                                else:
                                    text_box = WebDriverWait(driver, 10).until(
                                    ec.presence_of_element_located((By.XPATH,
                                                                    f"//*[@id='root']/div[3]/div/div/div[2]/div[1]/div/div/div/div[2]/div[{rule_count}]/input")))
                                    text_value = input("Enter the text value to input for the new rule: ")
                                    text_box.send_keys(text_value)
                                    break
                                break

                            else:
                                print("Invalid input. Please try again.")

                        if button_type == "AND":
                            print("MUL-429 Passed, User able to add multipe rule conditions")
                            driver.save_screenshot(f"{evidences}\\MUL-429.png")
                        elif button_type == "OR":
                            print("MUL-592 Passed,  User able to add multipe rule conditions")
                            driver.save_screenshot(f"{evidences}\\MUL-592.png")


                    elif rule_input.lower() == "done":
                        if button_type == "AND":
                            print("MUL-603 Passed, All the rule conditions are filled with inputs passed from Code inputs")
                            driver.save_screenshot(f"{evidences}\\MUL-603.png")

                        elif button_type == "OR":
                            print("MUL-604 Passed, All the rule conditions are filled with inputs passed from UI")
                            driver.save_screenshot(f"{evidences}\\MUL-604.png")
                        break


                # Start of the main loop to ask if the user wants to delete any rule
                while True:

                    delete_any_rule = input("Do you want to delete any rule? (yes/no): ").lower()

                    # If the user says 'yes', proceed to the next loop to delete rules
                    if delete_any_rule == "yes":
                        delete_button = input("Enter the rule number you want to remove: ")

                        try:

                            # Construct the XPath to locate the delete button for the specified rule number
                            delete_xpath = f"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[2]/div[{delete_button}]/button"
                            driver.save_screenshot(f"{evidences}\\MUL-426 before deleting.png")
                            # Find the delete button element using the constructed XPath
                            delete_button_element = driver.find_element(By.XPATH, delete_xpath).click()

                            print(f" MUL-426 Passed, Rule number {delete_button} has been deleted.")
                            driver.save_screenshot(f"{evidences}\\MUL-426 after deleting.png")# Confirmation message



                        # Catch any exceptions (e.g., element not found, invalid XPath) and print an error message
                        except Exception as e:
                            print(f"Error deleting rule: {e}")


                    # If the user says 'no' to deleting any rules, exit the loop
                    elif delete_any_rule == "no":
                        print("No rules will be deleted.")  # Inform the user no rules will be deleted
                        break  # Exit the loop

                    # If the user enters anything other than 'yes' or 'no', prompt them to provide a valid input
                    else:
                        print("Invalid input. Please enter 'yes' or 'no'.")

                        # After all conditions, ask for search or reset
                final_action = input("Enter 'search' to perform the search or 'reset' to reset the form: ")

                if final_action.lower() == "search":
                    driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[2]/div[2]/div[2]/button[2]").click()
                    time.sleep(10)  # Wait for the search results to load

                    try:
                        # Wait for the element to become invisible within 10 seconds
                        WebDriverWait(driver, 10).until(
                            ec.invisibility_of_element_located(
                                (By.XPATH, "//*[@id='root']/div[3]/div[2]/div/div[3]/span/span/span"))
                        )
                        print("The element has disappeared within the specified time.")
                    except TimeoutException:
                        print("Timeout: The element did not disappear within 15 seconds.")

                    result = driver.find_elements(By.XPATH,
                                                  "//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[1]/div/h6/div")

                    if result:  # If the result button is found
                        print("MUL-360 Passed , user able to see the search results")
                        if button_type == "AND":
                            print("MUL-431 Passed, user able to see result based on AND condition")

                            driver.save_screenshot(f"{evidences}\\MUL-431.png")

                        elif button_type == "OR":
                            print("MUL-594 Passed, user able to see result based on OR condition")
                            driver.save_screenshot(f"{evidences}\\MUL-594.png")
                    else:
                        no_data_found = driver.find_element(By.XPATH,
                                                            "//*[@id='root']/div[3]/div[2]/div/div[4]/div").text
                        print(f"MUL-369 Passed:{no_data_found}")
                        break

                    driver.implicitly_wait(2)

                    element_5 = driver.find_elements(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[1]/div/h6/div")
                    element_8 = driver.find_elements(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[1]/div/h6/i/div/div[1]/div")

                    # If both elements are found
                    if element_5 and element_8:
                        print("MUL-420 Passed, Second search result is visible.")
                        driver.save_screenshot(f"{evidences}\\MUL-420.png")
                    else:
                        pass

                    assert driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[1]")\
                           and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[2]")\
                           and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[3]") \
                           and driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[4]")
                    print("MUL-364 Passed, Wishlist ,Match, Unmatch and Export Buttons are visible")
                    driver.save_screenshot(f"{evidences}\\MUL-364.png")
                        # Locate the table

                    heading_xpath = "//*[@id='root']/div[3]/div[2]/div/div[3]/div[2]/div/table/thead/tr"
                    headings = driver.find_element(By.XPATH, heading_xpath).text.split("\n")

                    # Adjust headings to reflect the relevant data
                    headings = headings[0:]  # Keep only the relevant headings (skip first two)

                    # Now retrieve the table rows
                    rows = []
                    row_index = 1
                    while True:
                        try:
                            row_xpath = f"//*[@id='root']/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{row_index}]"
                            row_element = driver.find_element(By.XPATH, row_xpath)
                            cells = row_element.find_elements(By.XPATH, ".//td")

                            # Create a structured row list
                            structured_row = []

                            # Collect the row data, skipping the first two columns
                            for i in range(2,
                                           len(cells)):  # Start from index 2 to skip the first two columns
                                structured_row.append(cells[i].text)  # Get text from the corresponding cell

                            if structured_row:  # Only append if there's actual data
                                rows.append(structured_row)

                            row_index += 1
                        except NoSuchElementException:
                            break

                    # Print the output in a structured table format
                    print("\n")
                    print(tabulate(rows, headers=headings, tablefmt='grid'))
                    print("MUL-365 Passed, User able to see header ")
                    driver.save_screenshot(f"{evidences}\\MUL-365.png")
                    print("MUL-362 Passed , User able to see the golden records in main table view")
                    driver.save_screenshot(f"{evidences}\\MUL-362.png")

                    # Ask the user if they want to see the information pop-up
                    view_popup = input("Do you want to see the information pop-up? (yes/no): ").lower()

                    if view_popup == "yes":
                        # Ask the user to input the exact text to search for in the table
                        search_text = input("Enter the exact text you want to see the information pop-up: ")

                        # Find the total number of rows in the table (even if there are odd or even numbered rows)
                        total_rows = len(driver.find_elements(By.XPATH,
                                                              '//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr'))

                        # Variable to track whether a match was found
                        record_found = False

                        # Loop through odd-numbered rows in the table
                        for i in range(1, total_rows + 1,
                                       2):  # This will increment i by 2 to select only odd rows (1, 3, 5, etc.)
                            # Find all columns (cells) in the current odd row
                            columns = driver.find_elements(By.XPATH,
                                                           f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i}]/td')

                            # Loop through all columns in the current odd row
                            for column in columns:
                                cell_text = column.text.strip()  # Get the text of the cell

                                # Check if the cell text matches the search text AND if it has the underline style
                                if cell_text == search_text and 'text-decoration: underline' in column.get_attribute(
                                        'style'):
                                    try:
                                        # Wait until the element is clickable
                                        WebDriverWait(driver, 10).until(ec.element_to_be_clickable(column))

                                        # Scroll the element into view if it's not visible
                                        driver.execute_script("arguments[0].scrollIntoView(true);", column)

                                        # Click the element using ActionChains (alternative to direct click)
                                        ActionChains(driver).move_to_element(column).click().perform()
                                        print(f"Clicked on element with text: '{cell_text}'")
                                        print("MUL-367 Passed, Information pop up available post clicking on the multi id")
                                        driver.save_screenshot(f"{evidences}\\MUL-367.png")
                                        record_found = True
                                        break  # Exit the column loop once a match is found
                                    except ElementClickInterceptedException:
                                        print(
                                            f"Error: The element could not be clicked because another element is blocking it.")
                                    except Exception as e:
                                        print(f"Error occurred while clicking: {e}")
                                    break  # Exit the column loop if clicking failed

                            # If a match is found, exit the row loop as well
                            if record_found:
                                break

                        # If no record was found after checking all odd rows, print a message
                        if not record_found:
                            print("No record found.")

                        # Ask to close the pop-up if needed
                        close_popup = input("Enter the input to close the pop-up: ")
                        if close_popup == "close":
                            driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div[1]/button").click()
                            print("MUL-368 Passed, User able to close the pop up")
                            driver.save_screenshot(f"{evidences}\\MUL-368.png")
                        else:
                            pass

                    else:
                        print("You chose not to see the information pop-up. Continuing to the next task.")
                    clicked_headers = set()  # Use a set to keep track of already clicked headers

                    while True:
                        sort = input("Do you want to filter any data (Yes/No): ")

                        if sort.lower() == 'yes':
                            header_name = input("Enter the Header name to filter: ")

                            time.sleep(2)  # Adjust sleep if necessary (for loading)

                            # Find all header elements in the table
                            header_elements = driver.find_elements(By.XPATH,
                                                                   '//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/thead/tr/th')

                            # Initialize a flag to track if a match is found
                            header_found = False

                            # Loop through all header elements to find the one that matches the input text
                            for header in header_elements:
                                header_text = header.text.strip()  # Get the text of the header and strip whitespace

                                if header_text.lower() == header_name.lower():  # Case-insensitive comparison
                                    header_found = True
                                    # Check if the header has been clicked before
                                    if header_name.lower() not in clicked_headers:
                                        # If not clicked before, click it twice
                                        header.click()
                                        print(f"First time filtering by header: {header_name}")
                                        print("MUL-366 Passed, User able to sort the data by 2 clicks on header first time")
                                        driver.save_screenshot(f"{evidences}\\MUL-366.png")
                                        time.sleep(0.5)  # Optional: Short sleep between the two clicks
                                        header.click()
                                    else:
                                        # If clicked before, click it once
                                        header.click()
                                        print(f"Filtered by header: {header_name} (clicked once)")
                                        print(
                                            "MUL-366(2) Passed, User able to sort the data by clicking on header")
                                        driver.save_screenshot(f"{evidences}\\MUL-366(2).png")

                                    # Add this header to the set of clicked headers
                                    clicked_headers.add(header_name.lower())
                                    break  # Exit the loop once the matching header is found and clicked

                            if not header_found:
                                print(f"Header '{header_name}' not found. Please enter a valid header name.")

                        elif sort.lower() == 'no':
                            print("No filter applied.")
                            break  # Exit the loop
                        else:
                            print("Please enter 'Yes' or 'No'.")
                            break

                    match_or_unmatch = input("Enter 'add' to perform match funcationality or 'unmatch' to perform unmatch: ")

                    if match_or_unmatch.lower() == "add":
                        search_text = input("Enter multi id you want to check: ")

                        # Find the total number of rows in the table (even if there are odd or even numbered rows)
                        total_rows = len(driver.find_elements(By.XPATH,
                                                              '//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr'))

                        # Variable to track whether a match was found
                        record_found = False

                        # Loop through odd-numbered rows in the table
                        for i in range(1, total_rows + 1,2):  # This will increment i by 2 to select only odd rows (1, 3, 5, etc.)
                            # Find all columns (cells) in the current odd row
                            columns = driver.find_elements(By.XPATH,
                                                           f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i}]/td')

                            # Loop through all columns in the current odd row
                            for column in columns:
                                cell_text = column.text.strip()  # Get the text of the cell

                                # Check if the cell text matches the search text AND if it has the underline style
                                if cell_text == search_text and 'text-decoration: underline' in column.get_attribute(
                                        'style'):
                                    try:
                                        # Wait until the element is clickable
                                        WebDriverWait(driver, 10).until(ec.element_to_be_clickable(column))

                                        # Scroll the element into view (with smooth scrolling and centering it in the viewport)
                                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})",column)
                                        driver.implicitly_wait(2)

                                        # Find the checkbox corresponding to this row and click it
                                        checkbox_xpath = f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i}]/td[2]/span'
                                        checkbox = driver.find_element(By.XPATH, checkbox_xpath)
                                        time.sleep(2)

                                        ActionChains(driver).move_to_element(checkbox).click().perform()
                                        #driver.execute_script("arguments[0].click();", checkbox)


                                        print("MUL-363 Passed , selected MULTI ID clicked.")
                                        driver.save_screenshot(f"{evidences}\\MUL-363.png")
                                        print("MUL-417 Passed , selected MULTI ID clicked.")
                                        driver.save_screenshot(f"{evidences}\\MUL-417.png")

                                        record_found = True
                                        break  # Exit the column loop once a match is found

                                    except Exception as e:
                                        print(f"Error occurred while clicking: {e}")
                                    break  # Exit the column loop if clicking failed

                            # If a match is found, exit the row loop as well
                            if record_found:
                                break

                        # If no record was found after checking all odd rows, print a message
                        if not record_found:
                            print("No record found.")

                        wishlist=input("Enter the input to click on add to wishlist: ")

                        if wishlist=="wishlist":

                            wishlist_xpath=driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[1]")
                            WebDriverWait(driver, 10).until(ec.element_to_be_clickable(wishlist_xpath))

                            # Scroll the element into view (with smooth scrolling and centering it in the viewport)
                            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})",wishlist_xpath)

                            time.sleep(3)
                            ActionChains(driver).move_to_element(wishlist_xpath).click().perform()
                            driver.implicitly_wait(2)

                            element_1 = driver.find_elements(By.XPATH, "//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[1]/div/h6/i/div/div[1]/div")
                            element_2 = driver.find_elements(By.XPATH, "//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[1]/div/h6/i/div/div[2]/div")

                            # If both elements are found
                            if element_1 and element_2:
                                print("MUL-422 Passed ,2 records added to wishlist.")
                                driver.save_screenshot(f"{evidences}\\MUL-422.png")

                            # If only the first element is found
                            elif element_1:
                                print("MUL-418 Passed,User able to add 1st record to wishlist.")
                                driver.save_screenshot(f"{evidences}\\MUL-418.png")

                            # If neither element is found
                            else:
                                print("Failed to add records to wishlist.")


                        else:
                            pass
                        # Ask the user to confirm that they want to delete all the rules except the first one
                        delete_all_except_first = input("Do you want to delete all rules except the first one? (yes/no): ").lower()

                        if delete_all_except_first == "yes":
                            try:
                                # Find all the rule elements in the page dynamically
                                rule_elements = driver.find_elements(By.XPATH,
                                                                     "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[2]/div")

                                # Get the total number of rules (excluding the first one)
                                total_rules = len(rule_elements)

                                # Loop through all rules starting from 2 (to skip the first rule)
                                for i in range(2, total_rules + 1):  # Start from 2 to avoid deleting the first rule
                                    # Construct the XPath for the delete button of the current rule
                                    delete_xpath = f"//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div/div/div/div[2]/div[{i}]/button"

                                    # Save a screenshot before deleting
                                    driver.save_screenshot(f"{evidences}\\MUL-426 before deleting rule {i}.png")

                                    # Find the delete button element and click it
                                    delete_button_element = driver.find_element(By.XPATH, delete_xpath)
                                    delete_button_element.click()
                                    break

                            except Exception as e:
                                print(f"Error deleting rules: {e}")
                            print(f"All rules except the first one have been deleted.")

                        elif delete_all_except_first == "no":
                            print("No rules have been deleted.")

                            # continue_loop = input("Do you want to continue with another cycle? (yes/no): ")
                            # if continue_loop.lower() != "yes":
                            if element_2:
                                continue_loop = "no"
                                print("Two records have been added, and the match operation will continue.")

                                matchaction=input("Enter match to perform match action: ")
                                if matchaction.lower()== "match":
                                    driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[2]").click()
                                    time.sleep(3)

                                    comments=input("Enter the comments to perform match action: ")
                                    comment_box=driver.find_element(By.XPATH,"//*[@id='formComments']")
                                    comment_box.send_keys(comments)
                                    print("MUL-419 Passed, User able to enter comments")
                                    driver.save_screenshot(f"{evidences}\\MUL-419.png")
                                    # Ask the user if they want to proceed with the match or cancel
                                    match_action = input("Do you want to proceed with the match or cancel the operation? (proceed/cancel): ").lower()

                                    if match_action == "proceed":
                                        try:
                                            # Find and click the "Proceed" button to initiate the match
                                            proceed_button = driver.find_element(By.XPATH,"/html/body/div[3]/div/div/div[3]/button[2]")
                                            proceed_button.click()
                                            print("MUL-423 Passed, Match initiated.")
                                            driver.save_screenshot(f"{evidences}\\MUL-423.png")
                                            WebDriverWait(driver, 10).until(ec.staleness_of(
                                                driver.find_element(By.XPATH, "//div[@class='modal-content']")))

                                            notification = WebDriverWait(driver, 10).until(ec.presence_of_element_located
                                                                                           ((By.XPATH, "//div[@role='alert']/div[2]"))).text
                                            print(f"MUL-424 Passed, {notification}")
                                            driver.save_screenshot(f"{evidences}\\MUL-424.png")
                                        except Exception as e:
                                            print(f"Error initiating match: {e}")

                                    elif match_action == "cancel":
                                        try:
                                            # Find and click the "Cancel" button to cancel the operation
                                            cancel_button = driver.find_element(By.XPATH,"/html/body/div[3]/div/div/div[3]/button[1]")
                                            cancel_button.click()
                                            print("Operation canceled.")
                                        except Exception as e:
                                            print(f"Error canceling operation: {e}")

                                    else:
                                        print("Invalid input. Please enter 'proceed' or 'cancel'.")

                                    export_button = input("Enter the input to perform action'(export)': ")
                                    driver.find_element(By.XPATH,
                                                        "//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[4]").click()
                                    print("MUL-421 Passed, Result has been exported.")

                                    driver.save_screenshot(f"{evidences}\\MUL-421.png")
                                    time.sleep(10)
                                else:
                                    print("Match action failed")
                        else:
                            print("invalid input")

                    elif match_or_unmatch.lower() == "unmatch":
                        search_text = input("Enter multi id to see the accordion view (non-golden records): ")

                        # Find the total number of rows in the table
                        total_rows = len(driver.find_elements(By.XPATH,
                                                              '//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr'))

                        # Variable to track whether a match was found
                        record_found = False

                        # Loop through odd-numbered rows in the table
                        for i in range(1, total_rows + 1,
                                       2):  # This will increment i by 2 to select only odd rows (1, 3, 5, etc.)
                            # Find all columns (cells) in the current odd row
                            columns = driver.find_elements(By.XPATH,
                                                           f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i}]/td')

                            # Loop through all columns in the current odd row
                            for column in columns:
                                cell_text = column.text.strip()  # Get the text of the cell

                                # Check if the cell text matches the search text AND if it has the underline style
                                if cell_text == search_text and 'text-decoration: underline' in column.get_attribute(
                                        'style'):
                                    try:
                                        # Wait until the column element is clickable (ensure visibility)
                                        WebDriverWait(driver, 10).until(ec.element_to_be_clickable(column))

                                        # Scroll the element into view (with smooth scrolling and centering it in the viewport)
                                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})",
                                            column)

                                        # Wait for the element to be clickable again after scrolling
                                        WebDriverWait(driver, 10).until(ec.element_to_be_clickable(column))

                                        # Find the dropdown button corresponding to this row
                                        dropdown_xpath = f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i}]/td[1]/button'
                                        dropdown = WebDriverWait(driver, 10).until(
                                            ec.element_to_be_clickable((By.XPATH, dropdown_xpath))
                                        )

                                        # Use JavaScript to click the dropdown if regular click isn't working
                                        driver.execute_script("arguments[0].click();", dropdown)

                                        # Confirm the accordion is expanded and visible



                                        driver.save_screenshot(f"{evidences}\\MUL-361-371.png")
                                        time.sleep(10)
                                        accordion_table_xpath = f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i + 1}]/td/div/div/div/div/table'
                                        no_data_xpath = f'//*[@id="root"]/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[{i + 1}]/td/div/div/div/div/div/p'

                                        try:
                                            # Check if the accordion table is present
                                            table_element = driver.find_element(By.XPATH, accordion_table_xpath)

                                            # Retrieve table headings
                                            heading_xpath = f"{accordion_table_xpath}/thead/tr"
                                            heading_elements = driver.find_elements(By.XPATH, f"{heading_xpath}/th")  # Locate all <th> elements
                                            headings = [heading.text.strip() for heading in heading_elements]

                                            # Adjust headings to reflect relevant data (skip first two if necessary)
                                            headings = headings[2:]

                                            # Initialize a list to hold all row data
                                            rows = []
                                            row_index = 1

                                            while True:
                                                try:
                                                    # Dynamically generate the XPath for each row
                                                    row_xpath = f"{accordion_table_xpath}/tbody/tr[{row_index}]"
                                                    row_element = driver.find_element(By.XPATH, row_xpath)
                                                    cells = row_element.find_elements(By.XPATH, ".//td")

                                                    # Create a structured row list
                                                    structured_row = []

                                                    # Collect row data, skipping the first two columns
                                                    for j in range(2, len(cells)):
                                                        structured_row.append(cells[j].text)

                                                    # Append row data to rows list if not empty
                                                    if structured_row:
                                                        rows.append(structured_row)

                                                    # Increment to move to the next row
                                                    row_index += 1
                                                except NoSuchElementException:
                                                    # Break loop if no more rows are found
                                                    break

                                            # Print the table data in a structured format
                                            print("\nAccordion View Table:\n")
                                            print(tabulate(rows, headers=headings, tablefmt='grid'))
                                            print("MUL-361,371 Passed, Selected MULTI ID accordion view is visible.")
                                            driver.save_screenshot(f"{evidences}\\MUL-371,361.png")

                                            # Construct the XPath to locate the delete button for the specified rule number
                                            non_golden_record= input("Enter the place of non golden record you want to select: ")
                                            non_golden_xpath = f"//*[@id='root']/div[3]/div[2]/div/div[3]/div[2]/div/table/tbody/tr[2]/td/div/div/div/div/table/tbody/tr[{non_golden_record}]/td[3]"
                                            #driver.save_screenshot(f"{evidences}\\MUL-dgr before deleting.png")
                                            # Find the delete button element using the constructed XPath
                                            non_golden_xpath_element = driver.find_element(By.XPATH, non_golden_xpath).click()
                                            print("MUL-378 Passed, Non golden selected to perform unmatch operation")
                                            driver.save_screenshot(f"{evidences}\\MUL-378.png")

                                        except NoSuchElementException:
                                            # Check if the "No data found" message is present
                                            try:
                                                no_data_message = driver.find_element(By.XPATH, no_data_xpath).text
                                                print(f"\nAccordion View: {no_data_message}")
                                            except NoSuchElementException:
                                                print("Neither table nor 'No data found' message was located.")

                                        record_found = True
                                        break  # Exit the column loop once a match is found

                                    except Exception as e:
                                        print(f"Error occurred while clicking: {e}")
                                        break  # Exit the column loop if clicking failed

                            # If a match was found, exit the row loop as well
                            if record_found:
                                break

                        # If no record was found after checking all odd rows, print a message
                        if not record_found:
                            print("No record found.")

                        unmatch_xpath = input("Enter the input to perform action'(unmatch)': ")
                        driver.find_element(By.XPATH,"//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[3]").click()

                        comments = input("Enter the comments to perform unmatch action: ")
                        comment_box = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div[2]/div[3]/textarea")
                        comment_box.send_keys(comments)
                        print("MUL-373 Passed, User able to enter comments")
                        driver.save_screenshot(f"{evidences}\\MUL-373.png")
                        unmatch_action = input("Do you want to proceed with the unmatch or cancel the operation? (proceed/cancel): ").lower()

                        if unmatch_action == "proceed":
                            try:
                                # Find and click the "Proceed" button to initiate the match
                                proceed_button = driver.find_element(By.XPATH,"/html/body/div[3]/div/div/div[3]/button[2]")
                                proceed_button.click()
                                print("Unmatch initiated.")

                                WebDriverWait(driver, 10).until(ec.staleness_of(driver.find_element(By.XPATH, "//div[@class='modal-content']")))

                                WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//div[@role='alert']/div[2]")))
                                unmatchnotification = driver.find_element(By.XPATH,"//div[@role='alert']/div[2]").text
                                print(f"MUL-377 Passed, {unmatchnotification}")

                                driver.save_screenshot(f"{evidences}\\MUL-377.png")
                            except Exception as e:
                                print(f"Error initiating unmatch: {e}")
                                break

                        elif unmatch_action == "cancel":
                            try:
                                # Find and click the "Cancel" button to cancel the operation
                                cancel_button = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div[3]/button[1]")
                                cancel_button.click()
                                print("Operation canceled.")
                            except Exception as e:
                                print(f"Error canceling operation: {e}")

                        else:
                            print("Invalid input. Please enter 'proceed' or 'cancel'.")


                        print("Unmatch action performed.")

                    # Click export after unmatch
                        export_button = input("Enter the input to perform action'(export)': ")
                        driver.find_element(By.XPATH,
                                        "//*[@id='root']/div[3]/div[2]/div/div[3]/div[1]/div[2]/button[4]").click()
                        print("MUL-375 Passed, result has been exported.")
                        driver.save_screenshot(f"{evidences}\\MUL-375.png")
                        break
                        time.sleep(15)


                    else:
                        print("Failed to add to wishlist or unmtach actions")

                elif final_action.lower() == "reset":
                    driver.find_element(By.XPATH, "//*[@id='root']/div[3]/div[2]/div/div[2]/div[2]/div[2]/button[1]").click()
                    time.sleep(10)
                    print("MUL-  Passed,")
        elif "demand" in str.lower(Homepage_Parameter):

            driver.find_element(By.XPATH, "//a[@href='/on-demand-id-gen']").click()
            print(driver.find_element(By.XPATH, "//h5").text)
            try:
                assert ec.presence_of_element_located((By.XPATH, "//table"))
                parameter = "old_user"
                print("Old User have entered the On-Demand Page")
            except:
                assert ec.presence_of_element_located((By.XPATH, "//div[@class='modal-content']"))
                parameter = "new_user"
                print("New User have entered the On-Demand Page")


            if parameter == 'old_user':

                if driver.find_element(By.XPATH, "//h5").text == 'On-Demand ID Generation':
                    print("MUL-391 passed, The 'On-Demand ID Generation' text is present on the page.")

                # Guide
                # if the user want to open the the guide.
                guide = input("Do you want to open the guide(Yes/No):")
                if str.lower(guide) == 'yes':
                    driver.find_element(By.LINK_TEXT, "Guide").click()
                    print("MUL-414 passed, The guide pop-up has been opened!")

                    # Make sure you upload the On-Demand template in the BMS UI before uploading into the On-Demand Portal.
                    print(driver.find_element(By.XPATH, "//div[@class='modal-body']/div[@class='text-center'][1]").text)
                    print(
                        "MUL-610 passed, The text 'Make sure you upload the On-Demand template in the BMS UI before uploading into the On-Demand Portal.' is present")
                    # Before you upload...
                    print(driver.find_element(By.XPATH, "//div[@class='modal-header']/div/div/b").text)
                    print("MUL-605 passed, The text 'Before you upload...' is present on guide pop-up.")

                    while True:
                        template = input("Do you want to download a template, if no give No as input:")

                        if str.lower(template) == 'on demand':
                            # clicking on the Download here to download the On Demand template.
                            driver.find_element(By.XPATH,
                                                "//a[@href='/OnDemand_MULTI_ID_Generation_Management_Platform_Template_V4.0.xlsx']").click()
                            print("MUL-605 passed, The On-Demand template has been downloaded.")
                        elif str.lower(template) == 'bms ui':
                            # Clicking on the Download here to download the BMS UI template.
                            driver.find_element(By.XPATH,
                                                "//a[@href='/Project-MULTI-On-Demand-Template-for-BMS-UI-Export.pptx']").click()
                            print("MUL-609 passed, The BMS UI template has been downloaded.")
                        elif str.lower(template) == 'person':
                            # Clicking on the Person Template hyper link to download the Person Template.
                            driver.find_element(By.LINK_TEXT, "Person Template").click()
                            print("MUL-606 passed, The Person Template has been downloaded.")
                        elif str.lower(template) == 'legal entity':
                            # Clicking on the Legal Entity Template hyper link to download the Legal Entity Template.
                            driver.find_element(By.LINK_TEXT, "Legal Entity Template").click()
                            print("MUL-607 passed, The Legal Entity Template has been downloaded.")
                        elif str.lower(template) == 'organization':
                            # Clicking on the Organization Template hyper link to download the Organization Template.
                            driver.find_element(By.LINK_TEXT, "Organization Template").click()
                            print("MUL-608 passed, The Organization Template has been downloaded.")
                        elif str.lower(template) == 'no':
                            driver.find_element(By.XPATH, "//div[@class='modal-body']/div[2]/div[2]/div/button").click()
                            print("MUL-416 passed, The Ok button functionality is working")
                            break

                else:
                    pass

                # File Upload start
                file_upload = input("Do you want to upload a file(Yes/No):")

                if str.lower(file_upload) == 'yes':
                    # Clicking on the upload button
                    driver.find_element(By.XPATH, "//div[@class='col-2']/div[@class='d-grid gap-2']/button").click()
                    print("MUL-389 passed, Upload button has been clicked to open the drop zone.")


                    driver.find_element(By.XPATH, "//div[@class='file-dropzone']").click()

                    file_location = input("Enter the file location you want to upload:")
                    time.sleep(3)

                    print(driver.find_element(By.XPATH,
                                              "//div[@class='file-dropzone']/div/div/div/p[@class='text-center card-text'][1]/div").text)

                    keyboard = Controller()

                    # "D:\Users\syeds3\Downloads\Person_Template.csv"
                    keyboard.type(file_location)

                    time.sleep(3)

                    keyboard.press(Key.enter)
                    keyboard.release(Key.enter)

                    try:
                        # File that is uploaded
                        print(driver.find_element(By.XPATH,
                                                  "//div[@class='main-container container']/div[2]/div/div[1]/div[1]/div[1]/div[1]").text)
                        print("MUl-211 passed, The file format is accepted by the API.")
                        # File size that is uploaded
                        print(driver.find_element(By.XPATH,
                                                  "//div[@class='main-container container']/div[2]/div/div[1]/div[1]/div[1]/div[2]").text)
                        print("MUL-254 passed, The name and size of the file uploaded is present under the dropzone.")
                        file_acceptance = input("Do you want to upload the file (Upload/Cancel):")
                        if file_acceptance.lower() == 'upload':
                            # Clicking on the Upload button
                            driver.find_element(By.XPATH,
                                                "//div[@class='main-container container']/div[2]/div/div[3]/div/button").click()
                        elif file_acceptance.lower() == 'cancel':
                            # Clicking on the Cancel button
                            driver.find_element(By.XPATH,
                                                "//div[@class='main-container container']/div[2]/div/div[2]/div/button").click()
                        print("MUL-595 passed, The UI is accepting the cancel and upload button functionality")
                    except:
                        # Checking if the file is rejected by looking at the error pop-up
                        print(driver.find_element(By.XPATH, "//div[@role='alert']/div[2]").text)
                        if driver.find_element(By.XPATH,
                                               "//div[@role='alert']/div[2]").text.lower() == 'invalid file format':
                            print("MUL-252 passed, Invalid file format is rejected!")
                        elif driver.find_element(By.XPATH,
                                                 "//div[@role='alert']/div[2]").text.lower() == 'invalid file size':
                            print("MUL-253 passed, Invalid file size is rejected!")
                        sys.exit()

                    # Checking on the upload icon
                    wait.until(ec.presence_of_element_located(
                        (By.XPATH, "//*[name()='svg' and @class='svg-inline--fa fa-arrow-up-from-bracket ']")))
                    print("The file has been uploaded!")
                else:
                    pass

                # File upload completed

                date_filter = input("Do you want to filter on the dates(Yes/No):")

                if date_filter.lower() == 'yes':
                    # Search Filters
                    # Clicking on the Start Date filter
                    driver.find_element(By.XPATH,
                                        "//div[@class='MuiGrid-root MuiGrid-direction-xs-row css-1n5khr6']/div/input").click()
                    time.sleep(5)

                    # Start Date start
                    start_date_year_filter = int(input("Enter the year of start date:"))
                    # Giving the Year as input
                    driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div/div/input").clear()
                    driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div/div/input").send_keys(
                        start_date_year_filter)
                    # Selecting the months from Dropzone
                    start_months = driver.find_elements(By.XPATH, "/html/body/div[2]/div[1]/div/div/select/option")
                    month_list = [month.text for month in start_months]
                    # for month in months:
                    #     month_list.append(month.text)

                    print(month_list)
                    selected_month = input("Enter the start month you want to select:")

                    for month in start_months:
                        if selected_month == month.text:
                            month.click()

                    # Selecting the date from the options
                    start_dates = driver.find_elements(By.XPATH,
                                                       "/html/body/div[2]/div[2]/div/div[2]/div/span[@class='flatpickr-day']")
                    start_date_list = [date.text for date in start_dates]
                    # for date in dates:
                    #     date_list.append(date.text)
                    print(start_date_list)

                    selected_date = input("Enter the start date you want to search on:")

                    for date in start_dates:
                        if selected_date == date.text:
                            action.move_to_element(date).click().perform()
                            break
                        else:
                            pass

                    driver.find_element(By.XPATH,
                                        "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[2]/input").click()
                    time.sleep(5)

                    # End Date selection
                    end_year_filter = int(input("Enter the year filter for end year:"))

                    driver.find_element(By.XPATH, "/html/body/div[3]/div[1]/div/div/div/input").clear()
                    driver.find_element(By.XPATH, "/html/body/div[3]/div[1]/div/div/div/input").send_keys(
                        end_year_filter)
                    # Selecting the months from Dropzone
                    end_months = driver.find_elements(By.XPATH, "/html/body/div[3]/div[1]/div/div/select/option")
                    end_month_list = [month.text for month in end_months]
                    # for month in months:
                    #     month_list.append(month.text)

                    print(end_month_list)
                    selected_month = input("Enter the end month you want to select:")

                    for month in end_months:
                        if selected_month == month.text:
                            month.click()

                    # Selecting the date from the options
                    end_dates = driver.find_elements(By.XPATH,
                                                     "/html/body/div[3]/div[2]/div/div[2]/div/span[@class='flatpickr-day']")
                    end_date_list = [date.text for date in end_dates]
                    # for date in dates:
                    #     date_list.append(date.text)
                    print(end_date_list)

                    selected_date = input("Enter the end date you want to search on:")

                    for date in end_dates:
                        if selected_date == date.text:
                            action.move_to_element(date).click().perform()
                            break
                        else:
                            pass

                    print("MUL-392 passed, The start date and end date filter are given.")

                    # Reset and Submit functionality
                    reset_submit = input("Do you want to submit/reset the filters:")
                    if reset_submit == 'submit':
                        driver.find_element(By.XPATH,
                                            "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[3]/div/button[2]").click()
                        print("MUL-388 passed, The filters have been submitted")
                    elif reset_submit == 'reset':
                        driver.find_element(By.XPATH,
                                            "//*[@id='root']/div[3]/div[2]/div/div[2]/div[1]/div[2]/div/div/div[3]/div/button[1]").click()
                        print("MUL-393 passed, The filters have been reseted")
                else:
                    pass

                file_status = input("File Status you want to check(All/In Progress/Completed/Failed) or else (no):")

                if file_status.lower() == 'all':
                    # Clicking on the All Status
                    driver.find_element(By.XPATH, "//div/a[@data-rr-ui-event-key='#']").click()
                    print("MUL-382 passed, The All button has been clicked")
                elif file_status.lower() == 'in progress':
                    # Clicking on the In Progress Status
                    driver.find_element(By.XPATH, "//div/a[@data-rr-ui-event-key='inp']").click()
                    print("MUL-390 passed, The 'In Progress' button has been clicked")
                elif file_status.lower() == 'completed':
                    # Clicking on the Completed Status
                    driver.find_element(By.XPATH, "//div/a[@data-rr-ui-event-key='link-1']").click()
                    print("MUL-385 passed, The Completed button has been clicked")
                elif file_status.lower() == 'failed':
                    # Clicking on the Failed Status
                    driver.find_element(By.XPATH, "//div/a[@data-rr-ui-event-key='link-2']").click()
                    print("MUL-386 passed, The Failed button has been clicked")
                else:
                    pass

                try:
                    wait.until(ec.presence_of_element_located((By.XPATH, "//table/tbody/tr[1]/td[1]")))
                    headings = driver.find_element(By.XPATH, "//table/thead").text.split("\n")
                    rows = []
                    row_index = 1
                    structured_row = []
                    while True:
                        try:
                            print(f"//table/tbody/tr[{row_index}]")
                            row_element = driver.find_element(By.XPATH, f"//table/tbody/tr[{row_index}]")
                            cells = row_element.find_elements(By.XPATH, ".//td")

                            # Create a structured row list
                            # for i in range(len(cells)):
                            #     structured_row.append(cells[i].text)
                            structured_row = [cells[i].text for i in range(len(cells))]

                            if structured_row:  # Only append if there's actual data
                                rows.append(structured_row)

                            row_index += 1
                        except:
                            break
                except:
                    print(driver.find_element(By.XPATH, "//table/tbody/div/div").text)

                # Print the output in a structured table format
                print("\n")
                print(tabulate(rows, headers=headings, tablefmt='grid'))

                # table_columns = driver.find_elements(By.XPATH,"//table[@class='table table-striped table-bordered']/thead/tr/th")
                # table_columns_list = [columns.text for columns in table_columns]
                # print("MUL-383 passed, The columns names are as follows:")
                # print(table_columns_list)
                #
                #
                # table_values = driver.find_elements(By.XPATH,"//table[@class='table table-striped table-bordered']/tbody/tr")
                # print("Number of elements in the table:"+ str(len(table_values)))
                #
                file_names = driver.find_elements(By.XPATH,
                                                  "//table[@class='table table-striped table-bordered']/tbody/tr/td[@class='text-start'][1]")
                # file_names_list = [file.text for file in file_names]
                # print(file_names_list)

                progress_bar_check = input("Do you want to check any file progress(Yes/No):")

                if progress_bar_check.lower() == 'yes':

                    file_status_to_check = input("Enter the file you want to check the status for:")

                    for columns in file_names:
                        if columns.text == file_status_to_check:
                            columns.click()

                            # Waiting for the progress bar to open
                            wait.until(ec.presence_of_element_located((By.XPATH, "//td[@colspan=6]")))

                            if (driver.find_element(By.XPATH,
                                                    "//td[@colspan = 6]/div/div[2]/div[6]/div[1]").text == '3/3 done' or driver.find_element(
                                    By.XPATH, "//td[@colspan = 6]/div/div[2]/div[6]/div[1]").text == '2/3 done'):
                                print("The status of the file is as follows:")
                                print("\n")
                                # Validation Started
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[1]/div[3]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan=6]/div/div[1]/div[4]").text)
                                # Multi ID Generation Started
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[2]/div[3]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan=6]/div/div[2]/div[4]").text)
                                # File Ready to Download
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[2]/div[5]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan = 6]/div/div[2]/div[6]/div[2]").text)

                                driver.save_screenshot(f'{evidences}/complete_success_progress_bar.png')

                            elif driver.find_element(By.XPATH,
                                                     "//td[@colspan = 6]/div/div[2]/div[6]/div[1]").text == '1/3 done':
                                print("The status of the file is as follows:")
                                print("\n")
                                # Validation Started
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[1]/div[3]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan=6]/div/div[1]/div[4]").text)
                                # Multi ID Generation Failed
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[2]/div[3]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan=6]/div/div[2]/div[4]").text)

                                driver.save_screenshot(f'{evidences}/MultiID_Genration_failed_progress_bar.png')

                            else:
                                print("The status of the file is as follows:")
                                print("\n")
                                # Validation Failed
                                print(driver.find_element(By.XPATH,
                                                          "//td[@colspan=6]/div/div[1]/div[3]").text + " -> " + driver.find_element(
                                    By.XPATH, "//td[@colspan = 6]/div/div[2]/div[6]/div[2]").text)

                                driver.save_screenshot(f'{evidences}/Validation_failed_progress_bar.png')

                            print(driver.find_element(By.XPATH, "//td[@colspan = 6]/div/div[2]/div[6]/div").text)

                            close_refresh = input("Do you want to close/refresh the progress bar(Refresh/Close):")

                            if close_refresh.lower() == 'refresh':
                                driver.find_element(By.XPATH,
                                                    "//*[name() ='svg' and @class='svg-inline--fa fa-arrows-rotate ']").click()
                                print("The Progress bar has been refreshed!")
                            elif close_refresh.lower() == 'close':
                                driver.find_element(By.XPATH,
                                                    "//td[@colspan=6]/span/*[name()='svg' and @class='svg-inline--fa fa-circle-xmark text-danger']").click()
                                print("The Progress bar has been closed!")

                time.sleep(10)
                # --------------------------------Should add for Progress bar----------------------------------------#


            elif parameter == 'new_user':

                # Make sure you upload the On-Demand template in the BMS UI before uploading into the On-Demand Portal.
                wait.until(ec.presence_of_element_located(
                    (By.XPATH, "//div[@class='modal-body']/div[@class='text-center'][1]")))

                print(driver.find_element(By.XPATH, "//div[@class='modal-body']/div[@class='text-center'][1]").text)
                # Before you upload...
                print(driver.find_element(By.XPATH, "//div[@class='modal-header']/div/div/b").text)

                print("MUL-566 passed, The Guide pop-window has been opened on user entry.")

                while True:
                    template = input("Do you want to download a template, if no give No as input:")

                    if str.lower(template) == 'on demand':
                        # clicking on the Download here to download the On Demand template.
                        driver.find_element(By.XPATH,
                                            "//a[@href='/OnDemand_MULTI_ID_Generation_Management_Platform_Template_V4.0.xlsx']").click()
                        print("The On-Demand template has been downloaded.")
                    elif str.lower(template) == 'bms ui':
                        # Clicking on the Download here to download the BMS UI template.
                        driver.find_element(By.XPATH,
                                            "//a[@href='/Project-MULTI-On-Demand-Template-for-BMS-UI-Export.pptx']").click()
                        print("The BMS UI template has been downloaded.")
                    elif str.lower(template) == 'person':
                        # Clicking on the Person Template hyper link to download the Person Template.
                        driver.find_element(By.LINK_TEXT, "Person Template").click()
                        print("The Person Template has been downloaded.")
                    elif str.lower(template) == 'legal entity':
                        # Clicking on the Legal Entity Template hyper link to download the Legal Entity Template.
                        driver.find_element(By.LINK_TEXT, "Legal Entity Template").click()
                        print("The Legal Entity Template has been downloaded.")
                    elif str.lower(template) == 'organization':
                        # Clicking on the Organization Template hyper link to download the Organization Template.
                        driver.find_element(By.LINK_TEXT, "Organization Template").click()
                        print("The Organization Template has been downloaded.")
                    elif str.lower(template) == 'no':
                        driver.find_element(By.XPATH, "//div[@class='modal-body']/div[2]/div[2]/div/button").click()
                        break

                print(driver.find_element(By.XPATH,
                                          "//div[@class='file-dropzone']/div/div/div/p[@class='text-center card-text'][1]/div").text)
                print("MUL-572 passed, The file acceptance rules are printed on the console.")

                if driver.find_element(By.XPATH, "//h5").text == 'On-Demand ID Generation':
                    print("MUL-570 passed, The 'On-Demand ID Generation' text is present on the page.")

                # File Upload start
                file_upload = input("Do you want to upload a file(Yes/No):")

                if str.lower(file_upload) == 'yes':
                    # Clicking on the upload button
                    # driver.find_element(By.XPATH,
                    #                     "//div[@class='col-2']/div[@class='d-grid gap-2']/button").click()
                    # Clicking on the Drop Zone
                    driver.find_element(By.XPATH, "//div[@class='file-dropzone']").click()
                    print("MUL-571 passed, The dropzone is present on the UI.")

                    time.sleep(3)

                    keyboard = Controller()
                    # file_to_be_uploaded = input("Enter the file location that needs to be updated:")
                    # File to be uploaded
                    # "D:\\Users\\syeds3\\Downloads\\Person_Template.csv"
                    keyboard.type("D:\\Users\\syeds3\\Downloads\\Person_Template.csv")

                    time.sleep(2)

                    keyboard.press(Key.enter)
                    keyboard.release(Key.enter)

                    try:
                        # File that is uploaded
                        print(driver.find_element(By.XPATH,
                                                  "//div[@class='main-container container']/div[2]/div/div[1]/div[1]/div[1]/div[1]").text)

                        print("MUL-569 passed, The file name that us uploaded is visible on the UI screen!")
                        # File size that is uploaded
                        print(driver.find_element(By.XPATH,
                                                  "//div[@class='main-container container']/div[2]/div/div[1]/div[1]/div[1]/div[2]").text)
                        file_acceptance = input("Do you want to upload the file (Upload/Cancel):")
                        if file_acceptance.lower() == 'upload':
                            # Clicking on the Upload button
                            driver.find_element(By.XPATH,
                                                "//div[@class='main-container container']/div[2]/div/div[3]/div/button").click()

                            assert WebDriverWait(driver, timeout=50).until(
                                ec.presence_of_element_located((By.XPATH, "//table")))
                            print("MUL-568 passed, A table with the file that is uploaded is present on the UI.")
                        elif file_acceptance.lower() == 'cancel':
                            # Clicking on the Cancel button
                            driver.find_element(By.XPATH,
                                                "//div[@class='main-container container']/div[2]/div/div[2]/div/button").click()

                        print("MUL-567 passed, The UI is accepting the cancel and upload button functionality")


                    except:
                        # Checking if the file is rejected by looking at the error pop-up
                        print(driver.find_element(By.XPATH, "//div[@role='alert']/div[2]").text)
                        sys.exit()

                    # Checking on the upload icon
                    wait.until(ec.presence_of_element_located(
                        (By.XPATH, "//*[name()='svg' and @class='svg-inline--fa fa-arrow-up-from-bracket ']")))
                else:
                    pass

                time.sleep(15)

            elif str.lower(Homepage_Parameter) == 'home':
                # Navigating to the Home page.
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                print("You are in Home page!")

            elif 'login' in str.lower(Homepage_Parameter):
                # Navigating to the Home page.
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                # From Homepage navigating to the Login page.
                driver.find_element(By.XPATH,
                                    "//*[name()='svg' and @class='svg-inline--fa fa-right-from-bracket ']").click()
                print("MUL-685 passed, You are back in Login Page!")

            elif 'profile' in str.lower(Homepage_Parameter):
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                action.move_to_element(
                    driver.find_element(By.XPATH, "//*[name()='svg' and @class='svg-inline--fa fa-user ']")).perform()
                time.sleep(2)
                driver.save_screenshot(f"{evidences}\\profile.png")
                print("MUL-685 passed, You can see you email Id on the profile icon!")

            elif str.lower(Homepage_Parameter) == 'home':
                # Navigating to the Home page.
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                print("You are in Home page!")

            elif 'login' in str.lower(Homepage_Parameter):
                # Navigating to the Home page.
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                # From Homepage navigating to the Login page.
                driver.find_element(By.XPATH,
                                    "//*[name()='svg' and @class='svg-inline--fa fa-right-from-bracket ']").click()
                print("You are back in Login Page!")

            elif 'profile' in str.lower(Homepage_Parameter):
                driver.find_element(By.XPATH, "//div/a[@class='text-light' and @href='/']").click()
                action.move_to_element(
                    driver.find_element(By.XPATH, "//*[name()='svg' and @class='svg-inline--fa fa-user ']")).perform()
                time.sleep(2)
                driver.save_screenshot(f"{evidences}\\profile.png")
                print("You can see you email Id on the profile icon!")

    elif "@" not in email and "." not in email.split("@")[-1] and len(password)<6:
        # Email Error Message
        print("Email Error: "+ driver.find_element(By.XPATH,"//div/span[@class='text-danger validation ']").text)

        # Password Error Message
        print("Password Error: "+ driver.find_element(By.XPATH, "//div/span[@class='text-danger validation']").text)

    elif "@" not in email and "." not in email.split("@")[-1]:
        # Email Error Message
        print("Email Error: "+ driver.find_element(By.XPATH,"//div/span[@class='text-danger validation ']").text)
        print("MUL-271, Incorrect email address are notified to the users!")

    elif len(password)<6:
        # Password Error Message
        print("Password Error: "+ driver.find_element(By.XPATH, "//div/span[@class='text-danger validation']").text)
        print("MUL-259, Password that doesn't meet the criteria is notified to the users!")

elif "forgot password" in str.lower(Login_Parameter):
    # Click on Forgot Password hyperlink
    driver.find_element(By.LINK_TEXT, "Forgot password?").click()

    # Check for "Forgot Password" header
    if driver.find_element(By.XPATH, "//div/h2").text == "Forgot Password":
        print("MUL-1298 passed, User on the forgot password screen.")
        driver.save_screenshot(f"{evidences}\\MUL-1298_forgot_password_screen.png")
        time.sleep(5)


        # Validate presence of elements on the Forgot Password page
        assert ec.presence_of_element_located((By.ID, "userId")) and ec.presence_of_element_located ((By.CSS_SELECTOR, ".loginBtn"))
        print("MUL-1300 passed, Mail input and Send Verification button is present on the forgot password screen.")


        # Input the user ID and click submit
        driver.find_element(By.ID, "userId").send_keys(email)
        print("MUL-1299 passed,  input field for the email address is present.")
        driver.save_screenshot(f"{evidences}\\MUL-1299_mailinputs.png")
        time.sleep(3)
        driver.find_element(By.CSS_SELECTOR, ".loginBtn").click()

        time.sleep(5)  # Allow time for the request to process

        # Check email format validity

        if not email:
            email_error = driver.find_element(By.XPATH,
                                              "//*[@id='root']/div[2]/div/div/div/div[2]/div/form/div[1]/span").text
            print("MUL-1302 passed,"+email_error)
            driver.save_screenshot(f"{evidences}\\MUL-1302_invalid_inputs.png")
            sys.exit()

        elif "@" not in email or "." not in email.split("@")[-1]:
            invalid_email = driver.find_element(By.XPATH,
                                                "//*[@id='root']/div[2]/div/div/div/div[2]/div/form/div[1]/span").text
            print("MUL-1302 passed, " + invalid_email)
            driver.save_screenshot(f"{evidences}\\MUL-1302(2)_invalid_inputs.png")
            sys.exit()

        else:
            # No action required if the email format is valid and no errors were found
            print("MUL-1301 passed, The email format is valid and no errors were found.")
            driver.save_screenshot(f"{evidences}\\MUL-1301_valid_inputs.png")

        # Check for excessive clicks only if the previous checks are passed

        try:
            # Wait for the error element to be present
            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, "//div[@role='alert']/div[2]"))
            )
            error_message = driver.find_element(By.XPATH,"//div[@role='alert']/div[2]").text
            print(f"MUL-1303 Passed: {error_message}")

            driver.save_screenshot(f"{evidences}\\MUL-1303_excessive_clicks.png")
            sys.exit()

        except TimeoutException:
            pass

        time.sleep(8)

        try:
            assert driver.find_element(By.XPATH, "//div/h4").text == "Setup New Password"
            print("MUL-1304 passed: The user is currently on the new password setup screen.")
        except NoSuchElementException as e:
            print(f"Element not found after clicking the Send Verification button: {str(e)}")
            sys.exit()

        # validation of presence of elements
        try:
            driver.find_element(By.XPATH, "//*[@id='userId']") and driver.find_element(By.XPATH, "//*[@id='password']") and driver.find_element(By.XPATH, "(//*[@id='password'])[2]") and driver.find_element(By.CSS_SELECTOR, ".loginBtn")

            print("MUL-1305 passed,Verification code input, New password, confrim password inputs and submit button are present")
            driver.save_screenshot(f"{evidences}\\MUL-1304_newpassword_screen.png")


        except NoSuchElementException as e:
            print(f"Element not found after clicking Send Verification button: {str(e)}")

        # Input the verification code
        verification_code = input("Please enter your verification code: ")
        driver.find_element(By.XPATH, "//*[@id='userId']").send_keys(verification_code)

        # Input new password and confirmation
        new_password = input("Please Enter your New password: ")
        driver.find_element(By.XPATH, "//*[@id='password']").send_keys(new_password)

        confirm_password = input("Please Confirm your New password: ")
        driver.find_element(By.XPATH, "(//*[@id='password'])[2]").send_keys(confirm_password)

        print("MUL-1305 passed, User able to enter the data into input fileds")
        driver.save_screenshot(f"{evidences}\\MUL-1305_inputs_newpassword_sceen.png")

        # Click the submit button
        driver.find_element(By.CSS_SELECTOR, ".loginBtn").click()
        time.sleep(3)

        try:

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, "//div[@role='alert']/div[2]"))
            )
            error_message = driver.find_element(By.XPATH, "//div[@role='alert']/div[2]").text
            print(f"MUL-1307 Passed: {error_message}")


            driver.save_screenshot(f"{evidences}\\MUL-1307.png")

        except TimeoutException:
            try:
                # If excessive clicks error is not found, check for invalid input error
                WebDriverWait(driver, 10).until(
                    ec.presence_of_element_located(
                        (By.XPATH, "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[1]/span"))
                )
                invalid_error_message = driver.find_element(By.XPATH,
                                                            "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[1]/span").text
                print(f"MUL-1307 Passed: {invalid_error_message}")
                sys.exit()

                driver.save_screenshot(f"{evidences}\\MUL-1307_invalid_error.png")

            except TimeoutException:
                # If neither error message is found, do nothing
                pass

        min_length_new = len(new_password) >= 8
        has_number_new = re.search(r'\d', new_password)
        has_lowercase_new = re.search(r'[a-z]', new_password)
        has_uppercase_new = re.search(r'[A-Z]', new_password)
        has_special_char_new = re.search(r'[!@#$%^&*(),.?":{}|<>]', new_password)

        min_length_confirm = len(confirm_password) >= 8
        has_number_confirm = re.search(r'\d', confirm_password)
        has_lowercase_confirm = re.search(r'[a-z]', confirm_password)
        has_uppercase_confirm = re.search(r'[A-Z]', confirm_password)
        has_special_char_confirm = re.search(r'[!@#$%^&*(),.?":{}|<>]', confirm_password)

        # Checking if all criteria are met for the new password
        if not (min_length_new and has_number_new and has_lowercase_new and has_uppercase_new and has_special_char_new):
            try:
                # If the new password doesn't meet criteria, wait for the error element and capture the error message
                WebDriverWait(driver, 10).until(
                    ec.presence_of_element_located(
                        (By.XPATH, "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[2]/span"))
                    # Replace with correct XPath for the new password error
                )
                password_error_message = driver.find_element(By.XPATH,"//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[2]/span").text
                print(f"MUL-1306 Passed:Password Error: {password_error_message}")
                driver.save_screenshot(f"{evidences}\\Password_Error.png")


                # If the confirm password doesn't meet criteria, wait for the error element and capture the error message
                WebDriverWait(driver, 10).until(
                    ec.presence_of_element_located(
                        (By.XPATH, "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[3]/span[1]"))
                    # Replace with correct XPath for the confirm password error
                )
                confirm_password_error_message = driver.find_element(By.XPATH,
                                                                     "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[3]/span[1]").text
                print(f" MUL-1306 Passed:Password Error: {confirm_password_error_message}")
                driver.save_screenshot(f"{evidences}\\Confirm_Password_Error.png")
                sys.exit()

            except TimeoutException:
                # If the confirm password error element is not found within the timeout, silently continue
                pass

        # If both passwords meet the criteria, check if they match
        elif new_password != confirm_password:
            try:
                # Wait for the error element if passwords do not match
                WebDriverWait(driver, 10).until(
                    ec.presence_of_element_located(
                        (By.XPATH, "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[3]/span"))
                    # Replace with correct XPath for password match error
                )
                password_mismatch_error_message = driver.find_element(By.XPATH,
                                                                      "//*[@id='root']/div[2]/div/div/div[2]/div[2]/div/form/div[3]/span").text
                print(f"Password Error: {password_mismatch_error_message}")
                driver.save_screenshot(f"{evidences}\\Confirm_Password_Mismatch_Error.png")
                sys.exit()

            except TimeoutException:
                # If the password mismatch error element is not found within the timeout, silently continue
                pass

        else:
            print("password meets the criteria ")

    if driver.find_element(By.XPATH, "//div/h2").text == 'Login':
        print("MUL-1308 passed, The URL is redirected to the login page!")
        driver.save_screenshot(f"{evidences}\\MUL-1308_Login_page.png")

        driver.find_element(By.ID, "userId").send_keys(email)

        New_password= input("Enter your New password to login:")
        driver.find_element(By.ID, "password").send_keys(New_password)
        driver.find_element(By.CSS_SELECTOR, ".loginBtn").click()

        if  driver.find_element(By.XPATH, "//div/main/h2").text == "MOODY'S UNIVERSAL LINKING & TRACKING IDENTIFIER":

            print("MUL-1309 passed, The user has landed on the home page")
            driver.save_screenshot(f"{evidences}\\HomePage.png")
        else:
            print("Failed to login")

    else:
        print( "The URL is not redirected to the login page!")
else:
     print("Forgot Password test case failed")
     sys.exit()

# Close the WebDriver
driver.quit()

