SELECT Title AS Titre, product_id, _dates_event_user, Content, _wp_import, _address, _friendly_address, Image_Featured, Image_URL, _gallery_unserialized, Categories, Features, Author_ID as author_id
FROM `assofinder.crm.wp_export_associations`
WHERE _listing_type = '{0}'