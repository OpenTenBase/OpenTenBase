#include "postgres.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

static char *default_completion_model = NULL;
static char *default_embedding_model = NULL;
static char *default_image_model = NULL;

void _PG_init(void);

void _PG_fini(void);

void _PG_init(void)
{
    DefineCustomStringVariable(
        "ai.completion_model",
        "Sets the default AI completion model to use",
        "This parameter specifies which AI model will be used by default for text completions.",
        &default_completion_model,
        NULL,
        PGC_USERSET,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomStringVariable(
        "ai.embedding_model",
        "Sets the default AI embedding model to use",
        "This parameter specifies which AI model will be used by default for embeddings.",
        &default_embedding_model,
        NULL,
        PGC_USERSET,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomStringVariable(
        "ai.image_model",
        "Sets the default AI image model to use",
        "This parameter specifies which AI model will be used by default for image analysis.",
        &default_image_model,
        NULL,
        PGC_USERSET,
        0,
        NULL,
        NULL,
        NULL
    );
}

void _PG_fini(void)
{
    
}
