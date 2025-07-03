{% docs glific_prod_models %}

# Glific Production Models Documentation

## Overview
This document provides detailed information about the production-level models in the Glific data pipeline. These models handle contact information, training status, messaging metrics, and user activity data.

## Models

### 1. contact_flow_status_prod
**Purpose**: Tracks the training and flow status for each contact in the system.

**Key Features**:
- Combines contact information with training flow status
- Tracks both initiated and completed training states
- Extracts project information from LMS usernames

**Columns**:
- `contact_id` (PK): Unique identifier for each contact
- `phone`: Contact's phone number
- `project`: Project name (extracted from username_lms)
- `avni_gramin_initiated_flow` (0/1): Whether contact started AVNI Gramin training
- `rwb_training_initiated_flow` (0/1): Whether contact started RWB training
- `completed_both_trainings` (0/1): Whether both trainings are completed
- `avni_training_completed` (0/1): Whether AVNI training is completed
- `rwb_training_completed` (0/1): Whether RWB training is completed

### 2. training_split_prod
**Purpose**: Provides aggregated metrics about training completion status across all contacts.

**Key Features**:
- Categorizes contacts by training completion status
- Provides counts for each training category
- Ordered by training progression (Avni → Both → RWB → Not Started)

**Columns**:
- `training_type`: One of:
  * 'Avni Training Only'
  * 'RWB Training Only'
  * 'Both Trainings'
  * 'Training Not Started'
- `num_people`: Count of contacts in each category

### 3. flow_contexts_prod
**Purpose**: Tracks flow completion status for training programs.

**Key Features**:
- Monitors specific training flows using UUIDs
- Aggregates flow completion status per contact
- Identifies contacts who have completed both trainings

**Columns**:
- `contact_id` (PK): Unique identifier for each contact
- `avni_gramin_initiated_flow` (0/1): AVNI Gramin flow completion
- `rwb_training_initiated_flow` (0/1): RWB Training flow completion
- `completed_both_trainings` (0/1): Both flows completed

**Flow UUIDs**:
- AVNI Gramin: a0620d48-b427-4efe-870d-410051ef47c8
- RWB Training: 21b8db12-473e-403c-8daf-76575ffc2a66

### 4. active_dormant_users_prod
**Purpose**: Measures user engagement through message activity.

**Key Features**:
- Filters out test phone numbers (987654321*)
- Tracks both inbound messages and Hi Flow interactions
- Provides activity metrics per contact

**Columns**:
- `contact_phone` (PK): Contact's phone number
- `inbound_message_count`: Number of inbound messages
- `hi_flow_count`: Number of Hi Flow interactions

### 5. contacts_field_flatten_prod
**Purpose**: Provides flattened contact information with key metadata.

**Key Features**:
- Flattens JSON contact fields into columns
- Maintains contact role information
- Tracks contact creation and update timestamps

**Columns**:
- `contact_id` (PK): Unique identifier for each contact
- `role`: Contact's role in the system (nullable)
- `phone`: Contact's phone number
- `updated_date`: Last update timestamp
- `inserted_date`: Creation timestamp

### 6. message_conversations_prod
**Purpose**: Tracks message-level conversation data.

**Key Features**:
- Maintains conversation context
- Links messages to contacts and flows
- Inherits structure from message_conversations_int

### 7. messages_prod
**Purpose**: Tracks Hi Flow message interactions.

**Key Features**:
- Focuses on Hi Flow messages
- Aggregates message counts by contact
- Provides message-level engagement metrics

**Columns**:
- `contact_phone`: Contact's phone number
- `message_count`: Number of Hi Flow messages

## Dependencies
The production models follow this dependency chain:
1. Intermediate models (`*_int`) provide cleaned and transformed data
2. `contacts_field_flatten_prod` and `flow_contexts_prod` form the base
3. `contact_flow_status_prod` combines contact and flow information
4. `training_split_prod` aggregates the training status
5. Message-related models operate independently

## Usage Notes
- All binary flags use 0/1 values for consistency
- Phone numbers are validated to exclude test patterns
- Training status is tracked both at initiation and completion
- Timestamps use date format for consistency
- Contact IDs are cast to VARCHAR for joins to ensure type consistency

{% enddocs %} 