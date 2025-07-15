#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 定义表结构
typedef struct {
    int id;
    char name[50];
} Users;

typedef struct {
    int id;
    int user_id;  // 外键，关联Users表的id
    char content[100];
} Posts;

// 模拟数据库表
Users users[100];
Posts posts[100];
int user_count = 0;
int post_count = 0;

// 初始化测试数据
void init_test_data() {
    // 添加用户
    users[user_count++] = (Users){1, "Alice"};
    users[user_count++] = (Users){2, "Bob"};
    users[user_count++] = (Users){3, "Charlie"};
    
    // 添加帖子，关联到用户
    posts[post_count++] = (Posts){1, 1, "Hello world"};
    posts[post_count++] = (Posts){2, 1, "C programming"};
    posts[post_count++] = (Posts){3, 2, "Database design"};
}

// 简单的DELETE实现，带有关联删除
void delete_user(int user_id) {
    // 第一步：删除关联的帖子
    for (int i = 0; i < post_count; i++) {
        if (posts[i].user_id == user_id) {
            // 删除帖子（通过移动后面的元素覆盖）
            for (int j = i; j < post_count - 1; j++) {
                posts[j] = posts[j + 1];
            }
            post_count--;
            i--;  // 由于元素前移，需要重新检查当前位置
        }
    }
    
    // 第二步：删除用户
    for (int i = 0; i < user_count; i++) {
        if (users[i].id == user_id) {
            // 删除用户（通过移动后面的元素覆盖）
            for (int j = i; j < user_count - 1; j++) {
                users[j] = users[j + 1];
            }
            user_count--;
            break;
        }
    }
}

// 打印所有数据
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
    printf("=== 测试前数据 ===\n");
    init_test_data();
    print_all_data();
    
    printf("\n=== 执行关联删除 (删除用户ID=1) ===\n");
    delete_user(1);
    
    printf("\n=== 测试后数据 ===\n");
    print_all_data();
    
    return 0;
}
