from lark import Lark, Transformer, ParseError
from berkeleydb import db

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

#MyTransformer class
#쿼리를 parsing한 값을 보내 쿼리의 종류에 맞는 결과값을 출력함
#ex) create table quury 라면 'CREATE_TABLE' requested 출력

class MyTransformer(Transformer):
    def create_table_query(self, items):
        table_name = items[2].children[0].lower()
        column_definition_iter = items[3].find_data("column_definition")
        for column_definition in column_definition_iter:
            print(column_definition)
        print(items[1], items[2], items[3])
        print(table_name)
        print(column_definition_iter)
        #print("DB2020-11187> 'CREATE_TABLE' requested")
        for column_definition in column_definition_iter:
            print(column_definition)
    def drop_table_query(self, items):
        print("DB2020-11187> 'DROP_TABLE' requested")
    def explain_query(self, items):
        print("DB2020-11187> 'EXPLAIN' requested")
    def describe_query(self, items):
        print("DB2020-11187> 'DESCRIBE' requested")
    def desc_query(self, items):
        print("DB2020-11187> 'DESC' requested")
    def insert_query(self, items):
        print("DB2020-11187> 'INSERT' requested")
    def delete_query(self, items):
        print("DB2020-11187> 'DELETE' requested")
    def select_query(self, items):
        print("DB2020-11187> 'SELECT' requested")
    def show_tables_query(self, items):
        print("DB2020-11187> 'SHOW_TABLES' requested")
    def update_table_query(self, items):
        print("DB2020-11187> 'UPDATE' requested")


#입력을 처리하는 부분
exit_ = False   #exit이 실행되었는지 확인하는 exit_ 변수
while exit_ != 1:
    first_prompt = True  #처음 한 번만 프롬프트를 표시하기 위한 플래그

    # 입력 중간에 개행이 입력되어도 구문이 끝난 것이 아니라면 계속 입력을 받도록 input 값을 관리함
    input_lines = ""
    while not input_lines.endswith(';\n'):
        user_input = input("DB2020-11187> " if first_prompt else "")
        first_prompt = False
        input_lines += user_input + "\n"

    queries = []    #입력된 구문(들)을 semicolon(;)을 기준으로 분리하여 쿼리 관리
    queries = input_lines.split(";")

    for q in queries:
        error_flag = False    #입력 중에 Syntax error가 존재하는지 확인하는 플래그
        #exit이 입력되었다면 종료
        if q.strip() == "exit":
            exit_ = True
            break
        #parsing -> 오류 확인 -> 결과 출력
        elif q.strip():
            try:
                parsed_output = sql_parser.parse(q.strip() + ";")
                MyTransformer().transform(parsed_output)
            except ParseError as e:
                # 파싱 오류 처리
                error_flag = True
                print("DB2020-11187> Syntax Error")
        #error 발생 시 종료
        if error_flag:
            queries = []
            break
