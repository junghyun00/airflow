def get_sftp():
    print('sftp 작업을 시작합니다')

def regist(name, sex, *args):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타 옵션들 : {args}')

def regist2(name, sex, *a, **k):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타 옵션들 : {a}')
    email = k['email'] or None
    phone = k['phone'] or None
    if email:
        print(email)
    if phone:
        print(phone)