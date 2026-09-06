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
