#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// �����ṹ
typedef struct {
    int id;
    char name[50];
} Users;

typedef struct {
    int id;
    int user_id;  // ���������Users���id
    char content[100];
} Posts;

// ģ�����ݿ��
Users users[100];
Posts posts[100];
int user_count = 0;
int post_count = 0;

// ��ʼ����������
void init_test_data() {
    // ����û�
    users[user_count++] = (Users){1, "Alice"};
    users[user_count++] = (Users){2, "Bob"};
    users[user_count++] = (Users){3, "Charlie"};
    
    // ������ӣ��������û�
    posts[post_count++] = (Posts){1, 1, "Hello world"};
    posts[post_count++] = (Posts){2, 1, "C programming"};
    posts[post_count++] = (Posts){3, 2, "Database design"};
}

// �򵥵�DELETEʵ�֣����й���ɾ��
void delete_user(int user_id) {
    // ��һ����ɾ������������
    for (int i = 0; i < post_count; i++) {
        if (posts[i].user_id == user_id) {
            // ɾ�����ӣ�ͨ���ƶ������Ԫ�ظ��ǣ�
            for (int j = i; j < post_count - 1; j++) {
                posts[j] = posts[j + 1];
            }
            post_count--;
            i--;  // ����Ԫ��ǰ�ƣ���Ҫ���¼�鵱ǰλ��
        }
    }
    
    // �ڶ�����ɾ���û�
    for (int i = 0; i < user_count; i++) {
        if (users[i].id == user_id) {
            // ɾ���û���ͨ���ƶ������Ԫ�ظ��ǣ�
            for (int j = i; j < user_count - 1; j++) {
                users[j] = users[j + 1];
            }
            user_count--;
            break;
        }
    }
}

// ��ӡ��������
void print_all_data() {
    printf("Users:\n");
    for (int i = 0; i < user_count; i++) {
        printf("ID: %d, Name: %s\n", users[i].id, users[i].name);
    }
    
    printf("\nPosts:\n");
    for (int i = 0; i < post_count; i++) {
        printf("ID: %d, UserID: %d, Content: %s\n", 
               posts[i].id, posts[i].user_id, posts[i].content);
    }
}

int main() {
    printf("=== ����ǰ���� ===\n");
    init_test_data();
    print_all_data();
    
    printf("\n=== ִ�й���ɾ�� (ɾ���û�ID=1) ===\n");
    delete_user(1);
    
    printf("\n=== ���Ժ����� ===\n");
    print_all_data();
    
    return 0;
}
