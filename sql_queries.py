sql_get_content_file_for_preprocess = """
select
    c.id,
    c.tenant_id,
    c.academic_year_id,
    c.section_id,
    c.course_id,
    c.uploaded_by,
    c.file_url,
    c.file_type,
    rt.role_name as uploaded_by_role
from edbyteai.content c
join edbyteai.users u
    on u.id = c.uploaded_by
   and u.tenant_id = c.tenant_id
   and coalesce(u.is_deleted, false) = false
   and coalesce(u.is_active, true) = true
join edbyteai.role_types_config rt
    on rt.id = u.role_id
where c.id = %(content_file_id)s
  and c.tenant_id = %(tenant_id)s
  and c.academic_year_id = %(academic_year_id)s
  and c.section_id = %(section_id)s
  and c.course_id = %(course_id)s
  and coalesce(c.is_deleted, false) = false
"""


sql_lock_content_file_for_post_process = """
select
    c.id,
    c.tenant_id,
    c.academic_year_id,
    c.section_id,
    c.course_id,
    c.uploaded_by,
    c.file_url,
    c.file_type,
    c.file_name,
    rt.role_name as uploaded_by_role
from edbyteai.content c
join edbyteai.users u
    on u.id = c.uploaded_by
   and u.tenant_id = c.tenant_id
   and coalesce(u.is_deleted, false) = false
   and coalesce(u.is_active, true) = true
join edbyteai.role_types_config rt
    on rt.id = u.role_id
where c.id = %(content_file_id)s
  and c.tenant_id = %(tenant_id)s
  and c.academic_year_id = %(academic_year_id)s
  and c.section_id = %(section_id)s
  and c.course_id = %(course_id)s
  and coalesce(c.is_deleted, false) = false
for update
"""


sql_lock_content_extractions_for_post_process = """
select
    ce.id,
    ce.content_file_id,
    ce.chunk_number,
    ce.from_page,
    ce.to_page,
    ce.extracted_chunk_s3_file_url,
    ce.is_processed,
    ce.error,
    ce.is_deleted
from edbyteai.content_extraction ce
where ce.content_file_id = %(content_file_id)s
  and coalesce(ce.is_deleted, false) = false
order by ce.chunk_number, ce.from_page, ce.to_page, ce.id
for update
"""


sql_update_content_text_file_for_post_process = """
update edbyteai.content
set file_url = %(text_file_s3_key)s,
    updated_at = now()
where id = %(content_file_id)s
  and tenant_id = %(tenant_id)s
  and academic_year_id = %(academic_year_id)s
  and section_id = %(section_id)s
  and course_id = %(course_id)s
  and coalesce(is_deleted, false) = false
"""
